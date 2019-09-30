import com.beust.klaxon.Klaxon
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import lib.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.log4j.Logger
import java.time.Duration
import java.util.*

class Kafka2HBaseSpec: StringSpec(){

    private val log = Logger.getLogger(Kafka2HBaseSpec::class.toString())

    init {
        "Messages with new identifiers are written to hbase but not to dlq" {
            val hbase = HbaseClient.connect()
            val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.producerProps)
            val parser = MessageParser()
            val converter = Converter()
            val topic = uniqueTopicName()
            val startingCounter = waitFor { hbase.getCount(topic) }
            val dlqConsumer = dlqConsumer()
            val body = wellformedValidPayload()
            val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())
            val key = parser.generateKey(converter.convertToJson(getId().toByteArray()))
            log.info("Sending well-formed record to kafka topic '${String(topic)}'.")
            producer.sendRecord(topic, "key1".toByteArray(), body, timestamp)
            log.info("Sent well-formed record to kafka topic '${String(topic)}'.")

            val referenceTimestamp = converter.getTimestampAsLong(getISO8601Timestamp())
            val storedValue = waitFor { hbase.getCellBeforeTimestamp(topic, key, referenceTimestamp) }
            String(storedValue ?: "null".toByteArray()) shouldBe String(body)

            val counter = waitFor { hbase.getCount(topic) }
            counter shouldBe startingCounter + 1

            val records = dlqConsumer.poll(Duration.ofSeconds(10))
            records.count() shouldBe 0
        }

        "Messages with previously received identifiers are written as new versions to hbase but not to dlq" {
            val dlqConsumer = dlqConsumer()
            val hbase = HbaseClient.connect()
            val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.producerProps)

            val parser = MessageParser()
            val converter = Converter()
            val topic = uniqueTopicName()
            val startingCounter = waitFor { hbase.getCount(topic) }

            val body1 = wellformedValidPayload()
            val kafkaTimestamp1 = converter.getTimestampAsLong(getISO8601Timestamp())
            val key = parser.generateKey(converter.convertToJson(getId().toByteArray()))
            hbase.putVersion(topic, key, body1, kafkaTimestamp1)

            Thread.sleep(1000)
            val referenceTimestamp = converter.getTimestampAsLong(getISO8601Timestamp())
            Thread.sleep(1000)

            val body2 = wellformedValidPayload()
            val kafkaTimestamp2 = converter.getTimestampAsLong(getISO8601Timestamp())
            producer.sendRecord(topic, "key2".toByteArray(), body2, kafkaTimestamp2)

            val records = dlqConsumer.poll(Duration.ofSeconds(10))
            records.count() shouldBe 0

            val storedNewValue = waitFor { hbase.getCellAfterTimestamp(topic, key, referenceTimestamp) }
            storedNewValue shouldBe body2

            val storedPreviousValue = waitFor { hbase.getCellBeforeTimestamp(topic, key, referenceTimestamp) }
            storedPreviousValue shouldBe body1

            val counter = waitFor { hbase.getCount(topic) }
            counter shouldBe startingCounter + 2
        }

        "Malformed json messages are written to dlq topic" {
            val dlqConsumer = dlqConsumer()
            val converter = Converter()
            val topic = uniqueTopicName()
            val body = "junk".toByteArray()
            val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())
            val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.producerProps)
            producer.sendRecord(topic, "key3".toByteArray(), body, timestamp)
            val records = dlqConsumer.poll(Duration.ofSeconds(20))
            records.count() shouldBe 1
            val malformedRecord = MalformedRecord("key3", String(body), "Invalid json")
            val expected = Klaxon().toJsonString(malformedRecord)
            val actual = String(records.elementAt(0).value())
            actual shouldBe expected
        }

        "Invalid json messages as per the schema are written to dlq topic" {
            //        val log = Logger.getLogger(SchemaSpec::class.toString())
            val dlqConsumer = dlqConsumer()
            val converter = Converter()
            val topic = uniqueTopicName()
            val body = """{ "key": "value" } """.toByteArray()
            val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())
            val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.producerProps)
            producer.sendRecord(topic, "key3".toByteArray(), body, timestamp)
            val records = dlqConsumer.poll(Duration.ofSeconds(10))
            records.count() shouldBe 1
            val malformedRecord = MalformedRecord(
                "key3", String(body),
                "Invalid schema for key3:${String(topic)}:0:0: Message failed schema validation: '#: required key [message] not found'."
            )
            val expected = Klaxon().toJsonString(malformedRecord)
            val actual = String(records.elementAt(0).value())
            dlqConsumer.commitSync()
            actual shouldBe expected
        }
    }
    private fun dlqConsumer(): KafkaConsumer<ByteArray, ByteArray> {
        val dlqConsumer = KafkaConsumer<ByteArray, ByteArray>(testConsumerProperties())
        val log = Logger.getLogger("clearDown")
        dlqConsumer.subscribe(mutableListOf(Config.Kafka.dlqTopic))
        log.info("Clearing down DLQ '${Config.Kafka.dlqTopic}'.")
        val removed = dlqConsumer.poll(Duration.ofSeconds(10))
        log.info("Removed: '${removed.count()}' records from the DLQ.")
        dlqConsumer.commitSync()
        return dlqConsumer
    }

    private fun testConsumerProperties() = Properties().apply {
        put("bootstrap.servers", getEnv("K2HB_KAFKA_BOOTSTRAP_SERVERS") ?: "kafka:9092")
        put("key.deserializer", ByteArrayDeserializer::class.java)
        put("value.deserializer", ByteArrayDeserializer::class.java)
        put("group.id", UUID.randomUUID().toString())
        put("metadata.max.age.ms", getEnv("K2HB_KAFKA_META_REFRESH_MS") ?: "10000")
    }
}
