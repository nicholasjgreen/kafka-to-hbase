import org.apache.kafka.clients.consumer.KafkaConsumer
import sun.misc.Signal

suspend fun main() {
    val hbase = HbaseClient.connect()
    val kafka = KafkaConsumer<ByteArray, ByteArray>(Config.Kafka.consumerProps)

    // Read as many messages as possible until a signal is received
    val job = shovelAsync(kafka, hbase, Config.Kafka.pollTimeout)

    Signal.handle(Signal("INT")) { job.cancel() }
    Signal.handle(Signal("TERM")) { job.cancel() }
    job.await()
    hbase.close()
    kafka.close()
}
