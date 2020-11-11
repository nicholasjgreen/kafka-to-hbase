
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.measureTimeMillis
import sun.misc.Signal

class Shovel(private val consumer: KafkaConsumer<ByteArray, ByteArray>) {

    fun shovel(metadataClient: MetadataStoreClient,
                    archiveAwsS3Service: ArchiveAwsS3Service,
                    manifestAwsS3Service: ManifestAwsS3Service,
                    pollTimeout: Duration) {
        listOf("INT", "TERM").forEach(this::handleSignal)
        val parser = MessageParser()
        val validator = Validator()
        val converter = Converter()
        val listProcessor = ListProcessor(validator, converter)
        var batchCount = 0
        while (!closed.get()) {
            consumer.subscribe(Config.Kafka.topicRegex)
            logger.info("Polling", "timeout", "$pollTimeout", "subscription", Config.Kafka.topicRegex.pattern())
            val records = consumer.poll(pollTimeout)
            if (records.count() > 0) {
                HbaseClient.connect().use { hbase ->
                    val timeTaken = measureTimeMillis {
                        listProcessor.processRecords(hbase, consumer, metadataClient, archiveAwsS3Service, manifestAwsS3Service, parser, records)
                    }
                    logger.info("Processed batch", "time_taken", "$timeTaken", "size", "${records.count()}")
                }
            }

            if (batchCountIsMultipleOfReportFrequency(batchCount++)) {
                printLogs(consumer)
            }
        }
    }

    private fun handleSignal(signalName: String) {
        logger.info("Setting up $signalName handler.")
        Signal.handle(Signal(signalName)) {
            logger.info("'$it' signal received, cancelling job.")
            closed.set(true)
            consumer.wakeup()
        }
    }

    private fun printLogs(consumer: KafkaConsumer<ByteArray, ByteArray>) {
        consumer.metrics().filter { it.key.group() == "consumer-fetch-manager-metrics" }
            .filter { it.key.name() == "records-lag-max" }
            .map { it.value }
            .forEach { logger.info("Max record lag", "lag", it.metricValue().toString()) }

        consumer.listTopics()
            .filter { (topic, _) -> Config.Kafka.topicRegex.matcher(topic).matches() }
            .forEach { (topic, _) ->
                logger.info("Subscribed to topic", "topic_name", topic)
            }
    }


    fun batchCountIsMultipleOfReportFrequency(batchCount: Int): Boolean = (batchCount % Config.Shovel.reportFrequency) == 0

    companion object {
        private val logger = LoggerFactory.getLogger(Shovel::class.java)
        private val closed: AtomicBoolean = AtomicBoolean(false)
    }
}

