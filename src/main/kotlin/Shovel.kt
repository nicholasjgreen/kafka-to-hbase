import kotlinx.coroutines.*
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration

val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger("ShovelKt")

fun shovelAsync(consumer: KafkaConsumer<ByteArray, ByteArray>, hbase: HbaseClient, pollTimeout: Duration) =
    GlobalScope.async {
        val parser = MessageParser()
        val validator = Validator()
        val converter = Converter()
        val processor = RecordProcessor(validator, converter)
        val offsets = mutableMapOf<String, Long>()
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

                consumer.listTopics().forEach { (topic, _) ->
                    logger.info("Subscribed to topic", "topic_name", topic)
                }

                val records = consumer.poll(pollTimeout)


                if (records.count() > 0) {
                    logger.info("Processing records", "record_count", records.count().toString())
                    for (record in records) {
                        processor.processRecord(record, hbase, parser)
                        offsets[record.topic()] = record.offset()

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

            } catch (e: HbaseReadException) {
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
        throw HbaseReadException("Unable to reconnect to Hbase after $attempts attempts")
    }
}

fun printLogs(consumer: KafkaConsumer<ByteArray, ByteArray>, offsets: MutableMap<String, Long>, usedPartitions: MutableMap<String, MutableSet<Int>>) {
    logger.info("Total number of topics", "number_of_topics", offsets.size.toString())
    offsets.forEach { (topic, offset) ->
        logger.info("Offset", "topic_name", topic, "offset", offset.toString())
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
}

fun batchCountIsMultipleOfReportFrequency(batchCount: Int): Boolean {
    return (batchCount % Config.Shovel.reportFrequency) == 0
}
