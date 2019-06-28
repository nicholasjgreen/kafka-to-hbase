import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.PatternLayout

fun main() {
    val consoleAppender = ConsoleAppender()
    consoleAppender.layout = PatternLayout("%d [%p|%c|%C{1}] %m%n")
    consoleAppender.threshold = Level.INFO
    consoleAppender.activateOptions()
    Logger.getRootLogger().addAppender(consoleAppender)

    val logger = Logger.getLogger("testing-hbase")

    val hbase = HbaseClient(
        "hbase",
        2181,
        "k2hb",
        "cf".toByteArray(),
        "data".toByteArray()
    )

    val topic = "test-topic".toByteArray()
    val key = "my_key".toByteArray()

    hbase.createTopicTable(
        topic,
        10,
        1,
        300
    )

    hbase.putVersion(
        topic,
        key,
        "my_value_old".toByteArray(),
        20190626141700
    )

    hbase.putVersion(
        topic,
        key,
        "my_value_latest".toByteArray(),
        20190626151200
    )
}