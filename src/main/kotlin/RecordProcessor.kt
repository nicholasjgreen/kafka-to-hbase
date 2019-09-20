import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.logging.Logger
import com.beust.klaxon.JsonObject

class RecordProcessor() {
    fun processRecord(record: ConsumerRecord<ByteArray, ByteArray>, hbase: HbaseClient, parser: MessageParser, log: Logger) {
        var json: JsonObject
        val converter = Converter()

        try {
            json = converter.convertToJson(record.value())
        } catch (e: IllegalArgumentException) {
            log.warning("Could not parse message body for record with data of %s".format(
                    getDataStringForRecord(record)
                )
            )
            return
        }

        val formattedKey = parser.generateKeyFromRecordBody(json)

        if (formattedKey.isEmpty()) {
            log.warning(
                "Empty key was skipped for record with data of %s".format(
                    getDataStringForRecord(record)
                ))
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
            log.info(
                "Written record to HBase with data of %s".format(
                    getDataStringForRecord(record)
                )
            )
        } catch (e: Exception) {
            log.severe(
                "Error writing record to HBase with data of %s".format(
                    getDataStringForRecord(record)
                )
            )
            throw e
        }
    }
}

fun getDataStringForRecord(record: ConsumerRecord<ByteArray, ByteArray>) : String {
    return "%s:%s:%d:%d".format(
        String(record.key() ?: ByteArray(0)),
        record.topic(),
        record.partition(),
        record.offset()
    )
}
