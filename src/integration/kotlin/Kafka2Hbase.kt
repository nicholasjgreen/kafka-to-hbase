import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import lib.*
import org.apache.kafka.clients.producer.KafkaProducer

class Kafka2Hbase : StringSpec({
    configureLogging()
    
    val topic = "test-topic".toByteArray()

    val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.props)
    val hbase = HbaseClient.connect()

    "messages with new identifiers are written to hbase" {
        val startingCounter = waitFor { hbase.getCount(topic) }

        val body = uniqueBytes()
        val timestamp = timestamp()
        val key = uniqueBytes()
        producer.sendRecord(topic, key, body, timestamp)

        val storedValue = waitFor { hbase.getCellAfterTimestamp(topic, key, timestamp) }
        storedValue shouldBe body

        val counter = waitFor { hbase.getCount(topic) }
        counter shouldBe startingCounter + 1
    }

    "messages with previously received identifiers are written as new versions" {
        val startingCounter = waitFor { hbase.getCount(topic) }

        val body = uniqueBytes()
        val timestamp = timestamp()
        val key = uniqueBytes()
        hbase.putVersion(topic, key, body, timestamp)

        val newBody = uniqueBytes()
        val newTimestamp = timestamp() + 1 // Add one to ensure different timestamp
        producer.sendRecord(topic, key, newBody, newTimestamp)

        val storedNewValue = waitFor { hbase.getCellAfterTimestamp(topic, key, newTimestamp) }
        storedNewValue shouldBe newBody

        val storedPreviousValue = waitFor { hbase.getCellBeforeTimestamp(topic, key, newTimestamp) }
        storedPreviousValue shouldBe body

        val counter = waitFor { hbase.getCount(topic) }
        counter shouldBe startingCounter + 2
    }
})