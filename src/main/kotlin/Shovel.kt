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
                try {
                    hbase.putVersion(
                        topic = record.topic().toByteArray(),
                        key = record.key(),
                        body = record.value(),
                        version = record.timestamp()
                    )
                    log.info(
                        "Wrote %s:%d:%d with key %s".format(
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            String(record.key())
                        )
                    )
                } catch (e: Exception) {
                    log.severe(
                        "Error while writing message %s:%d:%d with key %s: %s".format(
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            String(record.key()),
                            e.toString()
                        )
                    )
                    throw e
                }
            }
        }
    }