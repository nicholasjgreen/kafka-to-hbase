import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.*
import java.time.Duration
import java.util.*

fun main() {
    val consoleAppender = ConsoleAppender()
    consoleAppender.layout = PatternLayout("%d [%p|%c|%C{1}] %m%n")
    consoleAppender.threshold = Level.DEBUG
    consoleAppender.activateOptions()
    Logger.getRootLogger().addAppender(consoleAppender)

    val logger = Logger.getLogger("testing-hbase")

    val hbase = HbaseClient(
        ConnectionFactory.createConnection(HBaseConfiguration.create().apply {
            this.set("hbase.zookeeper.quorum", "hbase")
            this.setInt("hbase.zookeeper.port", 2181)
        })!!,
        "k2hb",
        "cf".toByteArray(),
        "data".toByteArray()
    )

    hbase.createTopicTable(
        "test-topic".toByteArray(),
        maxVersions = 10,
        minVersions = 1,
        timeToLive = Duration.ofDays(10)
    )

    val kafka = KafkaConsumer<ByteArray, ByteArray>(Properties().apply {
        this["bootstrap.servers"] = "kafka:9092"
        this["group.id"] = "test"
        this["enable.auto.commit"] = "false"
    }).apply {
        this.subscribe(listOf("test-topic"))
    }

    try {
        for (record in kafka.consume(
            pollDuration = Duration.ofSeconds(1),
            maxQuietDuration = Duration.ofMinutes(1)
        )) {
            hbase.putVersion(
                topic = record.topic,
                key = record.key,
                body = record.value,
                version = record.timestamp
            )
        }
    } finally {
        hbase.close()
        kafka.close()
    }
}