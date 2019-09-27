import Config.Kafka.dlqTopic
import com.beust.klaxon.JsonObject
import com.beust.klaxon.Klaxon
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.util.logging.Logger

open class RecordProcessor(private val validator: Validator, private val converter: Converter) {
    open fun processRecord(record: ConsumerRecord<ByteArray, ByteArray>, hbase: HbaseClient, parser: MessageParser, log: Logger) {
        val json: JsonObject
        try {
            json = converter.convertToJson(record.value())
            validator.validate(json.toJsonString())
        } catch (e: IllegalArgumentException) {
            log.warning("Could not parse message body for record with data of ${getDataStringForRecord(record)}")
            sendMessageToDlq(record, "Invalid json")
            return
        } catch (ex: InvalidMessageException) {
            val msg = "Invalid schema for ${getDataStringForRecord(record)}: ${ex.message}"
            log.warning(msg)
            sendMessageToDlq(record, msg)
            return
        }

        val formattedKey = parser.generateKeyFromRecordBody(json)


        if (formattedKey.isEmpty()) {
            log.warning("Empty key was skipped for record with data of ${getDataStringForRecord(record)}")
            return
        }

        try {
            val lastModifiedTimestampStr = converter.getLastModifiedTimestamp(json)
            val lastModifiedTimestampLong = converter.getTimestampAsLong(lastModifiedTimestampStr)
            hbase.putVersion(
                topic = record.topic().toByteArray(),
                key = formattedKey,
                body = record.value(),
                version = lastModifiedTimestampLong
            )
            log.info("Written record '${String(formattedKey)}' to HBase with data of ${getDataStringForRecord(record)}")
        } catch (e: Exception) {
            log.severe("Error writing record to HBase with data of ${getDataStringForRecord(record)}")
            throw e
        }
    }

    open fun sendMessageToDlq(record: ConsumerRecord<ByteArray, ByteArray>, reason: String) {
        val body = record.value()
        val malformedRecord = MalformedRecord(String(record.key()), String(body), reason)
        try {
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
            log.info("metadata topic : ${metadata?.topic()} offset : ${metadata?.offset()}")
        } catch (e: Exception) {
            log.warning("Error while sending message to dlq : " +
                "key ${record.key()} from topic ${record.topic()} with offset ${record.offset()} : $e")
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
}

fun getDataStringForRecord(record: ConsumerRecord<ByteArray, ByteArray>): String {
    return "${String(record.key() ?: ByteArray(0))}:${record.topic()}:${record.partition()}:${record.offset()}"
}
