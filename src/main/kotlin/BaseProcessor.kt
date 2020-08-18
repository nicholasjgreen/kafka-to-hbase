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
        logger.warn("Error processing record, sending to dlq", "reason", reason, "key", String(record.key()))

        try {
            val malformedRecord = MalformedRecord(String(record.key()), String(body), reason)
            val jsonString = Klaxon().toJsonString(malformedRecord)
            val producerRecord =
                    ProducerRecord(dlqTopic,null,null, record.key(),jsonString.toByteArray(),null)
            val metadata = DlqProducer.getInstance()?.send(producerRecord)?.get()
            logger.info("Sending message to dlq","key", String(record.key()), "topic",
                    metadata?.topic().toString(), "offset", "${metadata?.offset()}")
        } catch (e: Exception) {
            logger.error("Error sending message to dlq",
                    "key", String(record.key()), "topic", record.topic(), "offset", "${record.offset()}")
            throw DlqException("Exception while sending message to DLQ : $e")
        }
    }

}
