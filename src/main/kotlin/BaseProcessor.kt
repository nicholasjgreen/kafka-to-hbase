import Config.Kafka.dlqTopic
import com.beust.klaxon.Klaxon
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

open class BaseProcessor(private val validator: Validator, private val converter: Converter) {

    open fun recordAsJson(record: ConsumerRecord<ByteArray, ByteArray>) =
        try {
            converter.convertToJson(record.value()).let { json ->
                validator.validate(json.toJsonString())
                json
            }
        } catch (e: IllegalArgumentException) {
            logger.warn("Could not parse message body", "record", getDataStringForRecord(record))
            sendMessageToDlq(record, "Invalid json")
            null
        } catch (e: InvalidMessageException) {
            logger.warn("Schema validation error", "record", getDataStringForRecord(record), "message", "${e.message}")
            sendMessageToDlq(record, "Invalid schema for ${getDataStringForRecord(record)}: ${e.message}")
            null
        }

    open fun sendMessageToDlq(record: ConsumerRecord<ByteArray, ByteArray>, reason: String) {
        val body = record.value()
        val originalTopic = record.topic().toString()
        val originalOffset = record.offset().toString()
        val recordKey: ByteArray? = if (record.key() != null) record.key() else null
        val stringKey = if (recordKey != null) String(recordKey) else "UNKNOWN"
        logger.warn("Error processing record, sending to dlq", "reason", reason, "key", stringKey)

        try {
            val malformedRecord = MalformedRecord(stringKey, String(body), reason)
            val jsonString = Klaxon().toJsonString(malformedRecord)
            val producerRecord =
                ProducerRecord(dlqTopic, null, null, recordKey, jsonString.toByteArray(), null)

            logger.info(
                "Sending message to dlq", "key", stringKey, "original_topic", originalTopic,
                "original_offset", originalOffset
            )
            val metadata = DlqProducer.getInstance().send(producerRecord)?.get()
            logger.info(
                "Sent message to dlq", "key", stringKey, "dlq_topic",
                metadata?.topic().toString(), "dlq_offset", "${metadata?.offset()}"
            )
        } catch (e: Exception) {
            logger.error(
                "Error sending message to dlq",
                "key", String(record.key()), "topic", record.topic(), "offset", "${record.offset()}"
            )
            throw DlqException("Exception while sending message to DLQ : $e")
        }
    }

    fun getDataStringForRecord(record: ConsumerRecord<ByteArray, ByteArray>) =
        "${String(record.key() ?: ByteArray(0))}:${record.topic()}:${record.partition()}:${record.offset()}"
}
