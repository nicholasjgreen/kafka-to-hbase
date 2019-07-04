import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.PatternLayout
import sun.misc.Signal
import java.time.Duration
import java.util.*

suspend fun main() {
    val consoleAppender = ConsoleAppender()
    consoleAppender.layout = PatternLayout("%d [%p|%c|%C{1}] %m%n")
    consoleAppender.threshold = Level.INFO
    consoleAppender.activateOptions()
    Logger.getRootLogger().addAppender(consoleAppender)

    // Connect to Hbase
    val hbase = HbaseClient(
        ConnectionFactory.createConnection(HBaseConfiguration.create().apply {
            this.set("hbase.zookeeper.quorum", "zookeeper")
            this.setInt("hbase.zookeeper.port", 2181)
        })!!,
        "k2hb",
        "cf".toByteArray(),
        "data".toByteArray()
    )

    // Create the topic tables
    hbase.createTopicTable(
        "test-topic".toByteArray(),
        maxVersions = 10,
        minVersions = 1,
        timeToLive = Duration.ofDays(10)
    )

    // Create a Kafka consumer
    val kafka = KafkaConsumer<ByteArray, ByteArray>(Properties().apply {
        put("bootstrap.servers", "kafka:9092")
        put("group.id", "test")
        put("enable.auto.commit", "false")
        put("key.deserializer", ByteArrayDeserializer::class.java)
        put("value.deserializer", ByteArrayDeserializer::class.java)
        put("auto.offset.reset", "earliest")
    }).apply {
        this.subscribe(listOf("test-topic"))
    }

    // Read as many messages as possible then quit
    val job = shovelAsync(kafka, hbase)

    // Handle signals gracefully and wait for completion
    Signal.handle(Signal("INT")) { job.cancel() }
    Signal.handle(Signal("TERM")) { job.cancel() }
    job.join()

    hbase.close()
    kafka.close()
}