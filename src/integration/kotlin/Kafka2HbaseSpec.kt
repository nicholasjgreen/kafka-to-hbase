import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.beust.klaxon.Klaxon
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import lib.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.log4j.Logger
import java.io.BufferedReader
import java.text.SimpleDateFormat
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
            val s3Client = getS3Client()
            val summaries = s3Client.listObjectsV2("kafka2s3", "prefix").objectSummaries
            summaries.forEach{s3Client.deleteObject("kafka2s3", it.key)}
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

            val summaries1 = s3Client.listObjectsV2("kafka2s3", "prefix").objectSummaries
            summaries1.size shouldBe 0
        }

        "Messages with previously received identifiers are written as new versions to hbase but not to dlq" {
            val s3Client = getS3Client()
            val summaries = s3Client.listObjectsV2("kafka2s3", "prefix").objectSummaries
            summaries.forEach{s3Client.deleteObject("kafka2s3", it.key)}

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

           val summaries1 = s3Client.listObjectsV2("kafka2s3", "prefix").objectSummaries
            summaries1.size shouldBe 0

            val storedNewValue = waitFor { hbase.getCellAfterTimestamp(topic, key, referenceTimestamp) }
            storedNewValue shouldBe body2

            val storedPreviousValue = waitFor { hbase.getCellBeforeTimestamp(topic, key, referenceTimestamp) }
            storedPreviousValue shouldBe body1

            val counter = waitFor { hbase.getCount(topic) }
            counter shouldBe startingCounter + 2
        }

        "Malformed json messages are written to dlq topic" {
            val s3Client = getS3Client()

            val converter = Converter()
            val topic = uniqueTopicName()
            val body = "junk".toByteArray()
            val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())
            val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.producerProps)
            producer.sendRecord(topic, "key3".toByteArray(), body, timestamp)
            val malformedRecord = MalformedRecord("key3", String(body), "Invalid json")
            val expected = Klaxon().toJsonString(malformedRecord)
            Thread.sleep(10_000)
            val s3Object =  s3Client.getObject("kafka2s3", "prefix/test-dlq-topic/${SimpleDateFormat("YYYY-MM-dd").format(Date())}/key3").objectContent
            val actual = s3Object.bufferedReader().use(BufferedReader::readText)
            actual shouldBe expected
        }

        "Invalid json messages as per the schema are written to dlq topic" {
            val s3Client = getS3Client()
            val converter = Converter()
            val topic = uniqueTopicName()
            val body = """{ "key": "value" } """.toByteArray()
            val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())
            val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.producerProps)
            producer.sendRecord(topic, "key4".toByteArray(), body, timestamp)
            Thread.sleep(10_000)
            val s3Object = s3Client.getObject("kafka2s3", "prefix/test-dlq-topic/${SimpleDateFormat("YYYY-MM-dd").format(Date())}/key4").objectContent
            val actual = s3Object.bufferedReader().use(BufferedReader::readText)
            val malformedRecord = MalformedRecord(
                "key4", String(body),
                "Invalid schema for key4:${String(topic)}:0:0: Message failed schema validation: '#: required key [message] not found'."
            )
            val expected = Klaxon().toJsonString(malformedRecord)
            actual shouldBe expected
        }
    }

    private fun getS3Client(): AmazonS3{
        return  AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration("http://aws-s3:4572", "eu-west-2"))
            .withClientConfiguration(ClientConfiguration().withProtocol(Protocol.HTTP))
            .withCredentials(
                AWSStaticCredentialsProvider(BasicAWSCredentials("aws-access-key", "aws-secret-access-key")))
            .withPathStyleAccessEnabled(true)
            .disableChunkedEncoding()
            .build()
    }

}
