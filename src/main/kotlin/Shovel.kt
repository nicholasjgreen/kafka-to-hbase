import kotlinx.coroutines.*
import org.apache.hadoop.hbase.client.HBaseAdmin
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
            try {
                validateHbaseConnection(hbase)

                log.info("Subscribing to '${Config.Kafka.topicRegex}'.")
                consumer.subscribe(Config.Kafka.topicRegex)
                log.info("Polling for '${pollTimeout}'.")
                val records = consumer.poll(pollTimeout)
                log.info("Processing '${records.count()}' records.")
                for (record in records) {
                    processor.processRecord(record, hbase, parser)
                }

            } catch (e: java.io.IOException) {
                log.error(e.message)
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
