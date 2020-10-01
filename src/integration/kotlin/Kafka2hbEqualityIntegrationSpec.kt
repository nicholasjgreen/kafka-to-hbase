import com.beust.klaxon.Klaxon
import com.google.gson.Gson
import com.google.gson.JsonObject
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import lib.*
import org.apache.kafka.clients.producer.KafkaProducer
import java.io.BufferedReader
import java.text.SimpleDateFormat
import java.util.*
import kotlin.time.ExperimentalTime

@ExperimentalTime
class Kafka2hbEqualityIntegrationSpec : StringSpec() {

    init {
        "Equality Messages with new identifiers are written to hbase but not to dlq" {
            val hbase = HbaseClient.connect()
            val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.producerProps)
            val parser = MessageParser()
            val converter = Converter()
            val topic = uniqueEqualityTopicName()
            val matcher = TextUtils().topicNameTableMatcher(topic)!!

            val namespace = matcher.groupValues[1]
            val tableName = matcher.groupValues[2]
            val qualifiedTableName = sampleQualifiedTableName(namespace, tableName)
            hbase.ensureTable(qualifiedTableName)
            val s3Client = getS3Client()
            val summaries = s3Client.listObjectsV2("kafka2s3", "prefix").objectSummaries
            summaries.forEach { s3Client.deleteObject("kafka2s3", it.key) }
            val summariesManifests = s3Client.listObjectsV2("manifests", "streaming").objectSummaries
            summariesManifests.forEach { s3Client.deleteObject("manifests", it.key) }

            verifyMetadataStore(0, topic, true)

            val body = wellFormedValidPayloadEquality()
            val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())
            val (recordId, hbaseKey) = parser.generateKey(converter.convertToJson(getEqualityId().toByteArray()))
            println("Sending well-formed record to kafka topic '$topic'.")
            producer.sendRecord(topic.toByteArray(), "key1".toByteArray(), body, timestamp)
            println("Sent well-formed record to kafka topic '$topic'.")
            val referenceTimestamp = converter.getTimestampAsLong(getISO8601Timestamp())
            val storedValue =
                waitFor { hbase.getCellBeforeTimestamp(qualifiedTableName, hbaseKey, referenceTimestamp) }
            String(storedValue!!) shouldBe Gson().fromJson(String(body), JsonObject::class.java).toString()
            val summaries1 = s3Client.listObjectsV2("kafka2s3", "prefix").objectSummaries
            summaries1.size shouldBe 0
            val summariesManifests1 = s3Client.listObjectsV2("manifests", "streaming").objectSummaries
            summariesManifests1.size shouldBe 0


            verifyMetadataStore(1, topic, true)
        }

        "Equality Messages with previously received identifiers are written as new versions to hbase but not to dlq" {
            val s3Client = getS3Client()
            val summaries = s3Client.listObjectsV2("kafka2s3", "prefix").objectSummaries
            summaries.forEach { s3Client.deleteObject("kafka2s3", it.key) }

            val hbase = HbaseClient.connect()
            val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.producerProps)
            val parser = MessageParser()
            val converter = Converter()
            val topic = uniqueEqualityTopicName()
            val matcher = TextUtils().topicNameTableMatcher(topic)!!
            val namespace = matcher.groupValues[1]
            val tableName = matcher.groupValues[2]
            val qualifiedTableName = sampleQualifiedTableName(namespace, tableName)
            val kafkaTimestamp1 = converter.getTimestampAsLong(getISO8601Timestamp())
            val (recordId, hbaseKey) = parser.generateKey(converter.convertToJson(getEqualityId().toByteArray()))
            val body1 = wellFormedValidPayloadEquality()
            hbase.putVersion(qualifiedTableName, hbaseKey, body1, kafkaTimestamp1)

            verifyMetadataStore(0, topic, true)

            delay(1000)
            val referenceTimestamp = converter.getTimestampAsLong(getISO8601Timestamp())
            delay(1000)

            val body2 = wellFormedValidPayloadEquality()
            val kafkaTimestamp2 = converter.getTimestampAsLong(getISO8601Timestamp())
            producer.sendRecord(topic.toByteArray(), "key2".toByteArray(), body2, kafkaTimestamp2)

            val summaries1 = s3Client.listObjectsV2("kafka2s3", "prefix").objectSummaries
            summaries1.size shouldBe 0

            val storedNewValue =
                waitFor { hbase.getCellAfterTimestamp(qualifiedTableName, hbaseKey, referenceTimestamp) }
            Gson().fromJson(
                String(storedNewValue!!),
                JsonObject::class.java
            ) shouldBe Gson().fromJson(String(body2), JsonObject::class.java)

            val storedPreviousValue =
                waitFor { hbase.getCellBeforeTimestamp(qualifiedTableName, hbaseKey, referenceTimestamp) }
            String(storedPreviousValue!!) shouldBe String(body1)

            verifyMetadataStore(1, topic, true)
        }

        "Equality Malformed json messages are written to dlq topic" {
            val s3Client = getS3Client()
            val converter = Converter()
            val topic = uniqueEqualityTopicName()
            val body = "junk".toByteArray()
            val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())
            val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.producerProps)
            producer.sendRecord(topic.toByteArray(), "key3".toByteArray(), body, timestamp)
            val malformedRecord = MalformedRecord("key3", String(body), "Invalid json")
            val expected = Klaxon().toJsonString(malformedRecord)

            delay(10_000L)
            val s3Object = s3Client.getObject(
                "kafka2s3",
                "prefix/test-dlq-topic/${SimpleDateFormat("YYYY-MM-dd").format(Date())}/key3"
            ).objectContent
            val actual = s3Object.bufferedReader().use(BufferedReader::readText)
            actual shouldBe expected

            verifyMetadataStore(0, topic, true)
        }

        "Equality Invalid json messages as per the schema are written to dlq topic" {
            val s3Client = getS3Client()
            val converter = Converter()
            val topic = uniqueEqualityTopicName()
            val body = """{ "key": "value" } """.toByteArray()
            val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())
            val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.producerProps)
            producer.sendRecord(topic.toByteArray(), "key4".toByteArray(), body, timestamp)
            delay(10_000)
            val s3Object = s3Client.getObject(
                "kafka2s3",
                "prefix/test-dlq-topic/${SimpleDateFormat("YYYY-MM-dd").format(Date())}/key4"
            ).objectContent
            val actual = s3Object.bufferedReader().use(BufferedReader::readText)
            val malformedRecord = MalformedRecord(
                "key4", String(body),
                "Invalid schema for key4:$topic:9:0: Message failed schema validation: '#: 6 schema violations found'."
            )
            val expected = Klaxon().toJsonString(malformedRecord)
            actual shouldBe expected

            verifyMetadataStore(0, topic, true)
        }
    }

}
