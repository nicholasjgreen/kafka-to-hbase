import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.isActive
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.Logger
import java.time.Duration

fun shovelAsync(consumer: KafkaConsumer<ByteArray, ByteArray>, hbase: HbaseClient, pollTimeout: Duration) =
    GlobalScope.async {
        val log = Logger.getLogger("shovel")
        log.info(Config.Kafka.reportTopicSubscriptionDetails())
        val parser = MessageParser()
        val validator = Validator()
        val converter = Converter()
        val processor = RecordProcessor(validator, converter)

        while (isActive) {
            log.info("Subscribing to '${Config.Kafka.topicRegex}'.")
            consumer.subscribe(Config.Kafka.topicRegex)
            log.info("Polling for '${pollTimeout}'.")
            val records = consumer.poll(pollTimeout)
            log.info("Processing '${records.count()}' records.")
            for (record in records) {
                processor.processRecord(record, hbase, parser)
            }
        }
    }
