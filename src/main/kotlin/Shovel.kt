import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.*

val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger("ShovelKt")

fun shovelAsync(consumer: KafkaConsumer<ByteArray, ByteArray>, metadataClient: MetadataStoreClient, pollTimeout: Duration) =
    GlobalScope.async {
        val parser = MessageParser()
        val validator = Validator()
        val converter = Converter()
        val processor = RecordProcessor(validator, converter)
        val listProcessor = ListProcessor(validator, converter)
        val offsets = mutableMapOf<String, Map<String, String>>()
        var batchCount = 0
        val usedPartitions = mutableMapOf<String, MutableSet<Int>>()
        while (isActive) {
            try {
                consumer.subscribe(Config.Kafka.topicRegex)
                logger.info("Polling","poll_timeout", pollTimeout.toString(), "topic_regex", Config.Kafka.topicRegex.pattern())
                val records = consumer.poll(pollTimeout)

                if (records.count() > 0) {
                    val then = Date().time
                    HbaseClient.connect().use { hbase ->
                        var succeeded = false
                        try {
                            if (Config.Shovel.processLists) {
                                listProcessor.processRecords(hbase, consumer, metadataClient, parser, records)
                                succeeded = true
                            }
                            else {
                                for (record in records) {
                                    //TODO: Implement saving record to the metadata store database before sending to hbase in case hbase loses it
                                    processor.processRecord(record, hbase, metadataClient, parser)
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
                                succeeded = true
                            }
                        } finally {
                            val now = Date().time
                            logger.info("Processed batch", "succeeded", "$succeeded", "size", "${records.count()}", "duration_ms", "${now - then}")
                        }
                    }
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
