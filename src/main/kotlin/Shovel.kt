import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.isActive
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.logging.Logger

fun shovelAsync(kafka: KafkaConsumer<ByteArray, ByteArray>, hbase: HbaseClient, pollTimeout: Duration) =
    GlobalScope.async {
        val log = Logger.getLogger("shovelAsync")

        log.info(Config.Kafka.reportTopicSubscriptionDetails())

        while (isActive) {
            kafka.subscribe(Config.Kafka.topicRegex)
            val records = kafka.poll(pollTimeout)
            for (record in records) {

                val newKey: ByteArray = record.key() ?: ByteArray(0)
                if (newKey.isEmpty()) {
                    log.warning(
                        "Empty key was skipped for %s:%d:%d".format(
                            record.topic() ?: "null",
                            record.partition(),
                            record.offset()
                        ))
                    continue
                }

                try {
                    hbase.putVersion(
                        topic = record.topic().toByteArray(),
                        key = record.key(),
                        body = record.value(),
                        version = record.timestamp()
                    )
                    log.info(
                        "Wrote key %s data %s:%d:%d".format(
                            String(newKey),
                            record.topic() ?: "null",
                            record.partition(),
                            record.offset()
                        )
                    )
                } catch (e: Exception) {
                    log.severe(
                        "Error while writing key %s data %s:%d:%: %s".format(
                            String(newKey),
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