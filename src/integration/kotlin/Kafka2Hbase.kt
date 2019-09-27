import Config.Kafka.pollTimeout
import com.beust.klaxon.Klaxon
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import lib.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer

class Kafka2Hbase : StringSpec({
    configureLogging()

    val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.producerProps)
    val consumer = KafkaConsumer<ByteArray, ByteArray>(Config.Kafka.consumerProps)
    val hbase = HbaseClient.connect()
    val parser = MessageParser()
    val converter = Converter()

    "messages with new identifiers are written to hbase but not to dlq" {
        val topic = uniqueTopicName()
        val startingCounter = waitFor { hbase.getCount(topic) }

        val body = uniqueBytes()
        val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())
        val key = parser.generateKey(converter.convertToJson(getId().toByteArray()))
        producer.sendRecord(topic, "key1".toByteArray(), body, timestamp)

        Thread.sleep(1000)
        val referenceTimestamp = converter.getTimestampAsLong(getISO8601Timestamp())

        val storedValue = waitFor { hbase.getCellBeforeTimestamp(topic, key, referenceTimestamp) }
        String(storedValue ?: "null".toByteArray()) shouldBe String(body)

        val counter = waitFor { hbase.getCount(topic) }
        counter shouldBe startingCounter + 1

        consumer.subscribe(mutableListOf(Config.Kafka.dlqTopic))
        val records = consumer.poll(pollTimeout)
        records.count() shouldBe 0
    }

    "messages with previously received identifiers are written as new versions to hbase but not to dlq" {
        val topic = uniqueTopicName()
        val startingCounter = waitFor { hbase.getCount(topic) }

        val body1 = uniqueBytes()
        val kafkaTimestamp1 = converter.getTimestampAsLong(getISO8601Timestamp())
        val key = parser.generateKey(converter.convertToJson(getId().toByteArray()))
        hbase.putVersion(topic, key, body1, kafkaTimestamp1)

        Thread.sleep(1000)
        val referenceTimestamp = converter.getTimestampAsLong(getISO8601Timestamp())
        Thread.sleep(1000)

        val body2 = uniqueBytes()
        val kafkaTimestamp2 = converter.getTimestampAsLong(getISO8601Timestamp())
        producer.sendRecord(topic, "key2".toByteArray(), body2, kafkaTimestamp2)

        consumer.subscribe(mutableListOf(Config.Kafka.dlqTopic))
        val records = consumer.poll(pollTimeout)
        records.count() shouldBe 0

        val storedNewValue = waitFor { hbase.getCellAfterTimestamp(topic, key, referenceTimestamp) }
        storedNewValue shouldBe body2

        val storedPreviousValue = waitFor { hbase.getCellBeforeTimestamp(topic, key, referenceTimestamp) }
        storedPreviousValue shouldBe body1

        val counter = waitFor { hbase.getCount(topic) }
        counter shouldBe startingCounter + 2
    }


    "Malformed json messages are written to dlq topic" {
        val topic = uniqueTopicName()

        val key = parser.generateKey(converter.convertToJson(getId().toByteArray()))
        val body = "junk".toByteArray()
        val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())
        producer.sendRecord(topic, "key3".toByteArray(), body, timestamp)
        consumer.subscribe(mutableListOf(Config.Kafka.dlqTopic))
        val records = consumer.poll(pollTimeout)
        val malformedRecord = MalformedRecord("key3", String(body), "Invalid json")
        val expected = Klaxon().toJsonString(malformedRecord)
        val actual = String(records.elementAt(0).value())
        actual shouldBe expected
    }

    "Invalid json messages as per the schema are written to dlq topic" {
        val topic = uniqueTopicName()

        val key = parser.generateKey(converter.convertToJson(getId().toByteArray()))
        val body = """
            {
                "key": "value"
            }
        """.trimIndent().toByteArray()

        val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())
        producer.sendRecord(topic, "key3".toByteArray(), body, timestamp)
        consumer.subscribe(mutableListOf(Config.Kafka.dlqTopic))
        val records = consumer.poll(pollTimeout)
        val malformedRecord = MalformedRecord("key3", String(body),
            "Invalid schema for key3:${String(topic)}:0:0: Message failed schema validation: '#: required key [message] not found'.")
        val expected = Klaxon().toJsonString(malformedRecord)

        val actual = String(records.elementAt(0).value())
        actual shouldBe expected
    }

})
