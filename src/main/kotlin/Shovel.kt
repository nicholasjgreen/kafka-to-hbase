import kotlinx.coroutines.*
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.Logger
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
        while (isActive) {
            try {
                validateHbaseConnection(hbase)

                logger.info("Subscribing", "topic_regex", Config.Kafka.topicRegex.pattern(),
                    "metadataRefresh", Config.Kafka.metadataRefresh())
                consumer.subscribe(Config.Kafka.topicRegex)
                logger.info("Polling", "poll_timeout", pollTimeout.toString())
                val records = consumer.poll(pollTimeout)
                logger.info("Processing records", "record_count", records.count().toString())
                for (record in records) {
                    processor.processRecord(record, hbase, parser)
                    offsets[record.topic()] = record.offset()
                }

                if (batchCount++ % 50 == 0) {
                    offsets.forEach { topic, offset ->
                        logger.info("Offset", "topic_name", topic, "offset", offset.toString())
                    }
                }

            } catch (e: java.io.IOException) {
                logger.error(e.message?: "")
                cancel(CancellationException("Cannot reconnect to Hbase", e))
            }
        }

    }

fun validateHbaseConnection(hbase: HbaseClient){
    val logger = Logger.getLogger("shovel")

    val maxAttempts = Config.Hbase.retryMaxAttempts
    val initialBackoffMillis = Config.Hbase.retryInitialBackoff

    var success = false
    var attempts = 0

    while (!success && attempts < maxAttempts) {
        try {
            HBaseAdmin.checkHBaseAvailable(hbase.connection.configuration)
            success = true
        }
        catch (e: Exception) {
            val delay: Long = if (attempts == 0) initialBackoffMillis
            else (initialBackoffMillis * attempts * 2)
            logger.warn("Failed to connect to Hbase on attempt ${attempts + 1}/$maxAttempts, will retry in $delay ms, if ${attempts + 1} still < $maxAttempts: ${e.message}" )
            Thread.sleep(delay)
        }
        finally {
            attempts++
        }
    }

    if (!success) {
        throw java.io.IOException("Unable to reconnect to Hbase after $attempts attempts")
    }
}
