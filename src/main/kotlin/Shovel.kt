import kotlinx.coroutines.*
import kotlinx.coroutines.channels.consumesAll
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration

val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger("ShovelKt")

fun shovelAsync(consumer: KafkaConsumer<ByteArray, ByteArray>, hbase: HbaseClient, metadataClient: MetadataStoreClient, pollTimeout: Duration) =
    GlobalScope.async {
        val parser = MessageParser()
        val validator = Validator()
        val converter = Converter()
        val processor = RecordProcessor(validator, converter)
        val offsets = mutableMapOf<String, Map<String, String>>()
        var batchCount = 0
        val usedPartitions = mutableMapOf<String, MutableSet<Int>>()
        while (isActive) {
            try {
                validateHbaseConnection(hbase)
                logger.debug(
                    "Subscribing",
                    "topic_regex", Config.Kafka.topicRegex.pattern(),
                    "metadata_refresh", Config.Kafka.metadataRefresh()
                )
                consumer.subscribe(Config.Kafka.topicRegex)

                logger.info(
                    "Polling",
                    "poll_timeout", pollTimeout.toString(),
                    "topic_regex", Config.Kafka.topicRegex.pattern()
                )

                val records = consumer.poll(pollTimeout)

                if (records.count() > 0) {
                    logger.info("Processing records", "record_count", records.count().toString())
                    for (record in records) {
                        //TODO: Implement saving record to the metadata store database before sending to hbase in case hbase loses it
                        processor.processRecord(record, hbase, parser)
                        offsets[record.topic()] = mutableMapOf(
                            "offset" to "${record.offset()}",
                            "partition" to "${record.partition()}"
                        )
                        val set =
                            if (usedPartitions.containsKey(record.topic())) usedPartitions[record.topic()] else mutableSetOf()
                        set?.add(record.partition())
                        usedPartitions[record.topic()] = set!!
                    }
                    logger.info("Committing offset")
                    consumer.commitSync()
                }

                if (batchCountIsMultipleOfReportFrequency(batchCount++)) {
                    printLogs(consumer, offsets, usedPartitions)
                }

            } catch (e: HbaseConnectionException) {
                logger.error("Error connecting to Hbase", e)
                cancel(CancellationException("Error connecting to Hbase ${e.message}", e))
            } catch (e: HbaseWriteException) {
                logger.error("Error writing to Hbase", e)
                cancel(CancellationException("Error writing to Hbase ${e.message}", e))
            } catch (e: Exception) {
                logger.error("Error reading from Kafka", e)
                cancel(CancellationException("Error reading from Kafka ${e.message}", e))
            }
        }
    }

fun validateHbaseConnection(hbase: HbaseClient) {
    val maxAttempts = Config.Hbase.retryMaxAttempts
    val initialBackoffMillis = Config.Hbase.retryInitialBackoff

    var success = false
    var attempts = 0

    while (!success && attempts < maxAttempts) {
        try {
            HBaseAdmin.checkHBaseAvailable(hbase.connection.configuration)
            success = true
        } catch (e: Exception) {
            val delay: Long = if (attempts == 0) initialBackoffMillis
            else (initialBackoffMillis * attempts * 2)
            logger.warn(
                "Failed to connect to Hbase after multiple attempts",
                "attempt", (attempts + 1).toString(),
                "max_attempts", maxAttempts.toString(),
                "retry_delay", delay.toString()
            )
            Thread.sleep(delay)
        } finally {
            attempts++
        }
    }

    if (!success) {
        throw HbaseConnectionException("Unable to reconnect to Hbase after $attempts attempts")
    }
}

fun printLogs(
    consumer: KafkaConsumer<ByteArray, ByteArray>,
    offsets: MutableMap<String, Map<String, String>>,
    usedPartitions: MutableMap<String, MutableSet<Int>>
) {
    logger.info("Total number of topics", "number_of_topics", offsets.size.toString())
    offsets.forEach { (topic, offset) ->
        logger.info(
            "Offset",
            "topic_name", topic,
            "offset", offset["offset"] ?: "NOT_SET",
            "partition", offset["partition"] ?: "NOT_SET"
        )
    }
    usedPartitions.forEach { (topic, ps) ->
        logger.info(
            "Partitions read from for topic",
            "topic_name", topic,
            "partitions", ps.sorted().joinToString(", ")
        )
    }

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

fun batchCountIsMultipleOfReportFrequency(batchCount: Int): Boolean {
    return (batchCount % Config.Shovel.reportFrequency) == 0
}
