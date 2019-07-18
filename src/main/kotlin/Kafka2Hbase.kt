import org.apache.kafka.clients.consumer.KafkaConsumer
import sun.misc.Signal

suspend fun main() {
    configureLogging()

    // Create a Kafka consumer
    val kafka = KafkaConsumer<ByteArray, ByteArray>(Config.Kafka.props)
    kafka.subscribe(Config.Kafka.topics)

    // Connect to Hbase and create the topic tables
    val hbase = HbaseClient.connect()

    // Read as many messages as possible until a signal is received
    val job = shovelAsync(kafka, hbase, Config.Kafka.pollTimeout)

    // Handle signals gracefully and wait for completion
    Signal.handle(Signal("INT")) { job.cancel() }
    Signal.handle(Signal("TERM")) { job.cancel() }
    job.await()

    hbase.close()
    kafka.close()
}