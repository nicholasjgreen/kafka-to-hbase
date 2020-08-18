import com.beust.klaxon.JsonObject
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream

open class RecordProcessor(validator: Validator, private val converter: Converter) : BaseProcessor(validator, converter) {

    private val textUtils = TextUtils()


    open fun processRecord(record: ConsumerRecord<ByteArray, ByteArray>, hbase: HbaseClient, parser: MessageParser) {

        recordAsJson(record)?.let { json ->
            val formattedKey = parser.generateKeyFromRecordBody(json)
            if (formattedKey.isEmpty()) {
                logger.warn("Empty key for record", "record", getDataStringForRecord(record))
                return
            }
            writeRecordToHbase(json, record, hbase, formattedKey)
        }
    }

    private fun writeRecordToHbase(json: JsonObject, record: ConsumerRecord<ByteArray, ByteArray>, hbase: HbaseClient, formattedKey: ByteArray) {
        try {
            val (lastModifiedTimestampStr, fieldTimestampCreatedFrom) = converter.getLastModifiedTimestamp(json)
            val message = json["message"] as JsonObject
            message["timestamp_created_from"] = fieldTimestampCreatedFrom

            val lastModifiedTimestampLong = converter.getTimestampAsLong(lastModifiedTimestampStr)
            val matcher = textUtils.topicNameTableMatcher(record.topic())
            if (matcher != null) {
                val namespace = matcher.groupValues[1]
                val tableName = matcher.groupValues[2]
                val qualifiedTableName = targetTable(namespace, tableName)
                logger.debug(
                        "Written record to hbase", "record", getDataStringForRecord(record),
                        "formattedKey", String(formattedKey)
                )
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

fun getDataStringForRecord(record: ConsumerRecord<ByteArray, ByteArray>): String {
    return "${String(record.key() ?: ByteArray(0))}:${record.topic()}:${record.partition()}:${record.offset()}"
}
