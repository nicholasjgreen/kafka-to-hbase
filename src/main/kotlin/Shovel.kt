
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import kotlin.system.measureTimeMillis

val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger("ShovelKt")

fun shovelAsync(consumer: KafkaConsumer<ByteArray, ByteArray>,
                metadataClient: MetadataStoreClient,
                archiveAwsS3Service: ArchiveAwsS3Service,
                manifestAwsS3Service: ManifestAwsS3Service,
                pollTimeout: Duration) =
    GlobalScope.async {
        val parser = MessageParser()
        val validator = Validator()
        val converter = Converter()
        val listProcessor = ListProcessor(validator, converter)
        var batchCount = 0
        while (isActive) {
            try {
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
            } catch (e: Exception) {
                logger.error("Error reading from kafka", e, "error", e.message ?: "")
                cancel(CancellationException("Error reading from kafka ${e.message}", e))
            }
        }
    }


fun printLogs(consumer: KafkaConsumer<ByteArray, ByteArray>) {

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

