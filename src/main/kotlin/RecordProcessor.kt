import com.beust.klaxon.JsonObject
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream

open class RecordProcessor(validator: Validator, private val converter: Converter) : BaseProcessor(validator, converter) {

    private val textUtils = TextUtils()

    open fun processRecord(record: ConsumerRecord<ByteArray, ByteArray>, hbase: HbaseClient,
                           metadataStoreClient: MetadataStoreClient, parser: MessageParser) {
        recordAsJson(record)?.let { json ->
            val (unformattedId, formattedKey) = parser.generateKeyFromRecordBody(json)
            if (formattedKey.isEmpty()) {
                logger.warn("Empty key for record", "record", getDataStringForRecord(record))
                return
            }
            writeRecordToHbase(json, record, hbase, metadataStoreClient, formattedKey)

        }
    }

    private fun writeRecordToHbase(json: JsonObject, record: ConsumerRecord<ByteArray, ByteArray>,
                                   hbase: HbaseClient, metadataStoreClient: MetadataStoreClient, formattedKey: ByteArray) {
        try {
            val (lastModifiedTimestampStr, fieldTimestampCreatedFrom) = converter.getLastModifiedTimestamp(json)
            val message = json["message"] as JsonObject
            message["timestamp_created_from"] = fieldTimestampCreatedFrom

            val lastModifiedTimestampLong = converter.getTimestampAsLong(lastModifiedTimestampStr)
            val matcher = textUtils.topicNameTableMatcher(record.topic())
            if (matcher != null) {
                metadataStoreClient.recordProcessingAttempt(textUtils.printableKey(formattedKey), record, lastModifiedTimestampLong)
                val namespace = matcher.groupValues[1]
                val tableName = matcher.groupValues[2]
                val qualifiedTableName = targetTable(namespace, tableName)
                val recordBodyJson = json.toJsonString()
                hbase.put(qualifiedTableName!!, formattedKey, recordBodyJson.toByteArray(), lastModifiedTimestampLong)
            } else {
                logger.error("Could not derive table name from topic", "topic", record.topic())
            }
        } catch (e: Exception) {
            logger.error("Error writing record to HBase", e, "record", getDataStringForRecord(record))
            throw HbaseWriteException("Error writing record to HBase: $e")
        }
    }

    private fun targetTable(namespace: String, tableName: String) =
            textUtils.coalescedName("$namespace:$tableName")?.replace("-", "_")

    fun getObjectAsByteArray(obj: MalformedRecord): ByteArray? {
        val bos = ByteArrayOutputStream()
        val oos = ObjectOutputStream(bos)
        oos.writeObject(obj)
        oos.flush()
        return bos.toByteArray()
    }

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(RecordProcessor::class.toString())
    }

}

