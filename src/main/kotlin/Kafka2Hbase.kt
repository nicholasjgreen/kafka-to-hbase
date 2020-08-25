
import kotlinx.coroutines.Deferred
import org.apache.kafka.clients.consumer.KafkaConsumer
import sun.misc.Signal

suspend fun main() =
    MetadataStoreClient.connect().use { metadataStore ->
        KafkaConsumer<ByteArray, ByteArray>(Config.Kafka.consumerProps).use { kafka ->
            val awsS3Service = AwsS3Service.connect()
            val job = shovelAsync(kafka, metadataStore, awsS3Service, Config.Kafka.pollTimeout)
            handleSignal(job, "INT")
            handleSignal(job, "TERM")
            job.await()
        }
    }

private fun handleSignal(job: Deferred<Unit>, signalName: String) {
    Signal.handle(Signal(signalName)) {
        logger().info("Signal received, cancelling job", "signal", "$it")
        job.cancel()
    }
}

private fun logger() = JsonLoggerWrapper.getLogger("Kafka2HBase")
