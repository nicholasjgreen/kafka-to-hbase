import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import lib.*

class Kafka2Hbase : StringSpec({
    configureLogging()
    
    val topic = "test-topic".toByteArray()

    val producer = KafkaTestProducer()
    val hbase = HbaseTestClient()

    "messages with new identifiers are written to hbase" {
        val body = uniqueBytes()
        val timestamp = timestamp()
        val key = uniqueBytes()
        producer.sendRecord(topic, key, body, timestamp)

        val storedValue = waitFor { hbase.getCellAfterTimestamp(topic, key, timestamp) }
        storedValue shouldBe body
    }

    "messages with previously received identifiers are written as new versions" {
        val body = uniqueBytes()
        val timestamp = timestamp()
        val key = uniqueBytes()
        hbase.putCell(topic, key, timestamp, body)

        val newBody = uniqueBytes()
        val newTimestamp = timestamp() + 1 // Add one to ensure different timestamp
        producer.sendRecord(topic, key, newBody, newTimestamp)

        val storedNewValue = waitFor { hbase.getCellAfterTimestamp(topic, key, newTimestamp) }
        storedNewValue shouldBe newBody

        val storedPreviousValue = waitFor { hbase.getCellBeforeTimestamp(topic, key, newTimestamp) }
        storedPreviousValue shouldBe body
    }
})