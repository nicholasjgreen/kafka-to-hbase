
import io.prometheus.client.Gauge
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import sun.misc.Signal
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.measureTimeMillis
import kotlin.time.ExperimentalTime
import kotlin.time.minutes
import kotlin.time.seconds

@ExperimentalTime
class Shovel(private val consumer: KafkaConsumer<ByteArray, ByteArray>,
             private val k2hbRunningApplications: Gauge,
             private val maximumLagGauge: Gauge) {

    @ExperimentalTime
    suspend fun shovel(metadataClient: MetadataStoreClient,
                       corporateStorageService: CorporateStorageService,
                       manifestService: ManifestService,
                       pollTimeout: Duration) {
        k2hbRunningApplications.inc()
        listOf("INT", "TERM", "HUP").forEach(::handleSignal)
        val parser = MessageParser()
        val validator = Validator()
        val converter = Converter()
        val HBaseBypassFilter = HBaseBypassFilter(Config.Hbase.bypassTopics)
        val listProcessor =
            ListProcessor(validator, converter, MetricsClient.dlqTimer,
                MetricsClient.dlqRetries, MetricsClient.dlqFailures,
                MetricsClient.batchTimer, MetricsClient.batchFailures,
                MetricsClient.recordSuccesses, MetricsClient.recordFailures,
                HBaseBypassFilter)

        var batchCount = 0

        logger.info("Subscription regexes",
            "includes_regex" to Config.Kafka.topicRegex.pattern,
            "excludes_regex" to Config.Kafka.topicExclusionRegexText)

        while (!closed.get()) {
            try {
                SubscriberUtility.subscribe(consumer, Config.Kafka.topicRegex, Config.Kafka.topicExclusionRegex)

                logger.info("Polling", "timeout" to "$pollTimeout")

                val records = consumer.poll(pollTimeout)

                if (records.count() > 0) {
                    HbaseClient.connect().use { hbase ->
                        val timeTaken = measureTimeMillis {
                            listProcessor.processRecords(hbase, consumer, metadataClient, corporateStorageService,
                                manifestService, parser, records)
                        }
                        logger.info("Processed batch", "time_taken" to "$timeTaken", "size" to "${records.count()}")
                    }
                }

                if (batchCountIsMultipleOfReportFrequency(batchCount++)) {
                    printLogs(consumer)
                }
            } catch (e: WakeupException) {
                logger.info("Pool awoken")
            }
        }


        withTimeout(Config.Metrics.scrapeInterval + 1.minutes.inMilliseconds.toLong()) {
            while (!metricsDeleted.get()) {
                logger.info("Awaiting metrics deletion")
                delay(1.seconds)
            }
            logger.info("Metrics deleted")
        }
    }

    private fun handleSignal(signalName: String) {
        logger.info("Setting up signal handler.", "signal" to signalName)
        Signal.handle(Signal(signalName)) {
            logger.info("Signal received, cancelling job.", "signal" to "$it")
            closed.set(true)
            consumer.wakeup()
            k2hbRunningApplications.dec()
            MetricsClient.pushFinalMetrics()
            metricsDeleted.set(true)
        }
    }

    private fun printLogs(consumer: KafkaConsumer<ByteArray, ByteArray>) {
        consumer.metrics().filter { it.key.group() == "consumer-fetch-manager-metrics" }
            .filter { it.key.name() == "records-lag-max" }
            .mapNotNull { it.value }
            .forEach { metric ->
                val max = metric.metricValue() as Double
                if (!max.isNaN()) {
                    metric.metricName().tags().takeIf { tags ->
                        tags.containsKey("topic") && tags.containsKey("partition")
                    } ?.let { tags ->
                        logger.info("Max record lag", "lag" to "$max",
                            "topic" to "${tags["topic"]}", "partition" to "${tags["partition"]}")
                        maximumLagGauge.labels(tags["topic"], tags["partition"]).set(max)
                    }
                }
            }

        consumer.listTopics()
            .filter { (topic, _) -> Config.Kafka.topicRegex.matches(topic) }
            .forEach { (topic, _) ->
                logger.info("Subscribed to topic", "topic_name" to topic)
            }
    }

    fun batchCountIsMultipleOfReportFrequency(batchCount: Int): Boolean =
        (batchCount % Config.Shovel.reportFrequency) == 0

    companion object {
        private val logger = DataworksLogger.getLogger(Shovel::class)
        private val closed: AtomicBoolean = AtomicBoolean(false)
        private val metricsDeleted: AtomicBoolean = AtomicBoolean(false)
    }
}
