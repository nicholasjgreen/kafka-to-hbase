import org.apache.kafka.clients.consumer.KafkaConsumer
import sun.misc.Signal


suspend fun main() {
    val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger("Kafka2HBase")
    val hbase = HbaseClient.connect()
    KafkaConsumer<ByteArray, ByteArray>(Config.Kafka.consumerProps).use { kafka ->
        try {
            val job = shovelAsync(kafka, hbase, Config.Kafka.pollTimeout)

            Signal.handle(Signal("INT")) {
                logger.info("Int signal, cancelling job", "signal", "$it")
                job.cancel()
            }

            Signal.handle(Signal("TERM")) {
                logger.info("Term signal, cancelling job", "signal", "$it")
                job.cancel()
            }

            job.await()
        } finally {
            logger.info("Closing hbase connection")
            hbase.close()
            logger.info("Closed hbase connection")
        }
    }
}
