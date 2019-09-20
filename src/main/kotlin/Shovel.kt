import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.isActive
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.logging.Logger
import com.beust.klaxon.JsonObject

val log = Logger.getLogger("shovel")

fun shovelAsync(kafka: KafkaConsumer<ByteArray, ByteArray>, hbase: HbaseClient, pollTimeout: Duration) =
    GlobalScope.async {
        log.info(Config.Kafka.reportTopicSubscriptionDetails())

        val parser = MessageParser()
        val processor = RecordProcessor()

        while (isActive) {
            kafka.subscribe(Config.Kafka.topicRegex)
            val records = kafka.poll(pollTimeout)
            for (record in records) {
                processor.processRecord(record, hbase, parser, log)
            }
        }
    }
