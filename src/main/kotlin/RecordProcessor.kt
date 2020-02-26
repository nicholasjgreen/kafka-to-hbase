import Config.Kafka.dlqTopic
import com.beust.klaxon.JsonObject
import com.beust.klaxon.Klaxon
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream

open class RecordProcessor(private val validator: Validator, private val converter: Converter) {

    private val textUtils = TextUtils()

    open fun processRecord(record: ConsumerRecord<ByteArray, ByteArray>, hbase: HbaseClient, parser: MessageParser) {
        val json: JsonObject
        try {
            json = converter.convertToJson(record.value())
            validator.validate(json.toJsonString())
        } catch (e: IllegalArgumentException) {
            logger.warn("Could not parse message body", "record", getDataStringForRecord(record))
            sendMessageToDlq(record, "Invalid json")
            return
        } catch (ex: InvalidMessageException) {
            logger.warn("Schema validation error", "record", getDataStringForRecord(record), "message", "${ex.message}")
            sendMessageToDlq(record, "Invalid schema for ${getDataStringForRecord(record)}: ${ex.message}")
            return
        }

        val formattedKey = parser.generateKeyFromRecordBody(json)

        if (formattedKey.isEmpty()) {
            logger.warn("Empty key for record", "record", getDataStringForRecord(record))
            return
        }

        try {
            val lastModifiedTimestampStr = converter.getLastModifiedTimestamp(json)
            val lastModifiedTimestampLong = converter.getTimestampAsLong(lastModifiedTimestampStr)
            val matcher = textUtils.topicNameTableMatcher(record.topic())
            if (matcher != null) {
                val namespace = matcher.groupValues[1]
                val tableName = matcher.groupValues[2]
                val qualifiedTableName = "$namespace:$tableName".replace("-", "_")
                logger.debug("Written record to hbase", "record", getDataStringForRecord(record),
                    "formattedKey", String(formattedKey))
                hbase.putVersion(qualifiedTableName, formattedKey, record.value(), lastModifiedTimestampLong)
            }
            else {
                logger.error("Could not derive table name from topic", "topic", record.topic())
            }
        } catch (e: Exception) {
            logger.error("Error writing record to HBase", "record", getDataStringForRecord(record))
            throw e
        }
    }

    open fun sendMessageToDlq(record: ConsumerRecord<ByteArray, ByteArray>, reason: String) {
        val body = record.value()
        try {
            val malformedRecord = MalformedRecord(String(record.key()), String(body), reason)
            val jsonString = Klaxon().toJsonString(malformedRecord)
            val producerRecord = ProducerRecord<ByteArray, ByteArray>(
                dlqTopic,
                null,
                null,
                record.key(),
                jsonString.toByteArray(),
                null
            )
            val metadata = DlqProducer.getInstance()?.send(producerRecord)?.get()
            logger.info("metadata topic : ${metadata?.topic()} offset : ${metadata?.offset()}")
        } catch (e: Exception) {
            logger.warn("Error sending message to dlq",
                "key", String(record.key()), "topic", record.topic(), "offset",  "${record.offset()}")
            throw DlqException("Exception while sending message to DLQ : $e")
        }
    }

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
