import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import lib.*
import org.apache.kafka.clients.producer.KafkaProducer

class Kafka2Hbase : StringSpec({
    configureLogging()

    val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.props)
    val hbase = HbaseClient.connect()

    val parser = MessageParser()
    val converter = Converter()

    "messages with new identifiers are written to hbase" {
        val topic = uniqueTopicName()
        val startingCounter = waitFor { hbase.getCount(topic) }

        val body = uniqueBytes()
        val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())
        val key = parser.generateKey(converter.convertToJson(getId().toByteArray()))
        producer.sendRecord(topic, "key1".toByteArray(), body, timestamp)

        Thread.sleep(100)
        val referenceTimestamp = converter.getTimestampAsLong(getISO8601Timestamp())

        val storedValue = waitFor { hbase.getCellBeforeTimestamp(topic, key, referenceTimestamp) }
        storedValue shouldBe body

        val counter = waitFor { hbase.getCount(topic) }
        counter shouldBe startingCounter + 1
    }

    "messages with previously received identifiers are written as new versions" {
        val topic = uniqueTopicName()
        val startingCounter = waitFor { hbase.getCount(topic) }

        val body1 = uniqueBytes()
        val kafkaTimestamp1 = converter.getTimestampAsLong(getISO8601Timestamp())
        val key = parser.generateKey(converter.convertToJson(getId().toByteArray()))
        hbase.putVersion(topic, key, body1, kafkaTimestamp1)

        Thread.sleep(100)
        val referenceTimestamp = converter.getTimestampAsLong(getISO8601Timestamp())

        val body2 = uniqueBytes()
        val kafkaTimestamp2 = converter.getTimestampAsLong(getISO8601Timestamp())
        producer.sendRecord(topic, "key2".toByteArray(), body2, kafkaTimestamp2)

        val storedNewValue = waitFor { hbase.getCellAfterTimestamp(topic, key, referenceTimestamp) }
        storedNewValue shouldBe body2

        val storedPreviousValue = waitFor { hbase.getCellBeforeTimestamp(topic, key, referenceTimestamp) }
        storedPreviousValue shouldBe body1

        val counter = waitFor { hbase.getCount(topic) }
        counter shouldBe startingCounter + 2
    }

    "messages with empty id are skipped" {
        val topic = uniqueTopicName()
        val startingCounter = waitFor { hbase.getCount(topic) }

        val body = uniqueBytesNoId()
        val timestamp = timestamp()
        producer.sendRecord(topic, "key".toByteArray(), body, timestamp)

        val counter = waitFor { hbase.getCount(topic) }
        counter shouldBe startingCounter
    }
})
