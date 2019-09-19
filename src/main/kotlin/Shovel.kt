import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.isActive
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.logging.Logger
import com.beust.klaxon.JsonObject

fun shovelAsync(kafka: KafkaConsumer<ByteArray, ByteArray>, hbase: HbaseClient, pollTimeout: Duration) =
    GlobalScope.async {
        val log = Logger.getLogger("shovelAsync")

        log.info(Config.Kafka.reportTopicSubscriptionDetails())

        while (isActive) {
            kafka.subscribe(Config.Kafka.topicRegex)
            val records = kafka.poll(pollTimeout)
            var json: JsonObject
            for (record in records) {
                try {
                    json = convertToJson(record.value())
                } catch (e: IllegalArgumentException) {
                    log.warning("Could not parse message body, record will be skipped")
                    continue
                }

                val id: ByteArray = getId(json)
                val keyNew = generateKey(json)

                val key = record.key()

                if (key.isEmpty()) {
                    log.warning(
                        "Empty key was skipped for %s:%d:%d".format(
                            record.topic() ?: "null",
                            record.partition(),
                            record.offset()
                        ))
                    continue
                }

                try {
                    val lastModifiedTimestampStr = getLastModifiedTimestamp(json)
                    val lastModifiedTimestampLong = getTimestampAsLong(lastModifiedTimestampStr)
                    hbase.putVersion(
                        topic = record.topic().toByteArray(),
                        key = record.key(),
                        body = record.value(),
                        version = lastModifiedTimestampLong //record.timestamp()
                    )
                    log.info(
                        "Wrote key %s data %s:%d:%d".format(
                            String(key),
                            record.topic() ?: "null",
                            record.partition(),
                            record.offset()
                        )
                    )
                } catch (e: Exception) {
                    log.severe(
                        "Error while writing key %s data %s:%d:%: %s".format(
                            String(key),
                            record.topic() ?: "null",
                            record.partition(),
                            record.offset(),
                            e.toString()
                        )
                    )
                    throw e
                }
            }
        }
    }

fun generateKey(json: JsonObject): ByteArray {
    val log = Logger.getLogger("generateKey")
    val jsonOrdered = sortJsonByKey(json)
    val base64EncodedString: String = encodeToBase64(jsonOrdered)
    val checksumBytes: ByteArray = generateFourByteChecksum(jsonOrdered)

    return checksumBytes.plus(base64EncodedString.toByteArray())
}

fun getId(json: JsonObject): ByteArray {
    val log = Logger.getLogger("generateKey")
    return ByteArray(0)
}
