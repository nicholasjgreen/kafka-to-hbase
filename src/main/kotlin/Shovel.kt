
import org.apache.kafka.clients.consumer.KafkaConsumer
import sun.misc.Signal
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.measureTimeMillis
import kotlin.time.ExperimentalTime

class Shovel(private val consumer: KafkaConsumer<ByteArray, ByteArray>) {

    @ExperimentalTime
    suspend fun shovel(metadataClient: MetadataStoreClient,
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

            SubscriberUtility.subscribe(consumer, Config.Kafka.topicRegex, Config.Kafka.topicExclusionRegex)

            logger.info("Polling", "timeout" to "$pollTimeout")
            val records = consumer.poll(pollTimeout)
            if (records.count() > 0) {
                HbaseClient.connect().use { hbase ->
                    val timeTaken = measureTimeMillis {
                        listProcessor.processRecords(hbase, consumer, metadataClient, archiveAwsS3Service, manifestAwsS3Service, parser, records)
                    }
                    logger.info("Processed batch", "time_taken" to "$timeTaken", "size" to "${records.count()}")
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
            .forEach { logger.info("Max record lag", "lag" to it.metricValue().toString()) }

        consumer.listTopics()
            .filter { (topic, _) -> Config.Kafka.topicRegex.matches(topic) }
            .forEach { (topic, _) ->
                logger.info("Subscribed to topic", "topic_name" to topic)
            }
    }


    fun batchCountIsMultipleOfReportFrequency(batchCount: Int): Boolean = (batchCount % Config.Shovel.reportFrequency) == 0

    companion object {
        private val logger = DataworksLogger.getLogger(Shovel::class.java.toString())
        private val closed: AtomicBoolean = AtomicBoolean(false)
    }
}

