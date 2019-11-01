import Config.Kafka.dlqTopic
import com.beust.klaxon.JsonObject
import com.beust.klaxon.Klaxon
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.log4j.Logger
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream

open class RecordProcessor(private val validator: Validator, private val converter: Converter) {

    private val log = Logger.getLogger(RecordProcessor::class.toString())

    open fun processRecord(record: ConsumerRecord<ByteArray, ByteArray>, hbase: HbaseClient, parser: MessageParser) {
        val json: JsonObject
        try {
            json = converter.convertToJson(record.value())
            validator.validate(json.toJsonString())
        } catch (e: IllegalArgumentException) {
            log.warn("Could not parse message body for record with data of ${getDataStringForRecord(record)}")
            sendMessageToDlq(record, "Invalid json")
            return
        } catch (ex: InvalidMessageException) {
            val msg = "Invalid schema for ${getDataStringForRecord(record)}: ${ex.message}"
            log.warn(msg)
            sendMessageToDlq(record, msg)
            return
        }

        val formattedKey = parser.generateKeyFromRecordBody(json)
        log.info("Formatted key for the record '${String(record.key())}' is '$formattedKey'")

        if (formattedKey.isEmpty()) {
            log.warn("Empty key was skipped for record with data of ${getDataStringForRecord(record)}")
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
            log.info("Written '${getDataStringForRecord(record)}' to HBase with formatted key as '${String(formattedKey)}'.")
        } catch (e: Exception) {
            log.error("Error writing record to HBase with data of ${getDataStringForRecord(record)}")
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
            log.warn("Error while sending message to dlq : " +
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
