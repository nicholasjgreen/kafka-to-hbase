
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.google.gson.Gson
import com.google.gson.JsonObject
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import lib.getISO8601Timestamp
import lib.sampleQualifiedTableName
import lib.sendRecord
import lib.verifyMetadataStore
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Table
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.log4j.Logger
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPInputStream
import kotlin.time.ExperimentalTime
import kotlin.time.minutes
import kotlin.time.seconds

@ExperimentalTime
class Kafka2hbIntegrationLoadSpec : StringSpec() {

    companion object {
        private val log = Logger.getLogger(Kafka2hbIntegrationLoadSpec::class.toString())
        private const val TOPIC_COUNT = 10
        private const val RECORDS_PER_TOPIC = 1_000
        private const val BATCHES_PER_TOPIC = 2
        private const val DB_NAME = "load-test-database"
        private const val COLLECTION_NAME = "load-test-collection"
    }

    init {
        "Many messages sent to many topics" {
            publishRecords()
            verifyHbase()
            verifyMetadataStore(TOPIC_COUNT * RECORDS_PER_TOPIC, DB_NAME, false)
            verifyS3()
            verifyManifests()
        }
    }

    private fun publishRecords() {
        val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.producerProps)
        val converter = Converter()
        logger.info("Starting record producer...")
        repeat(TOPIC_COUNT) { collectionNumber ->
            val topic = topicName(collectionNumber)
            repeat(RECORDS_PER_TOPIC) { messageNumber ->
                val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())
                logger.info("Sending record $messageNumber/$RECORDS_PER_TOPIC to kafka topic '$topic'.")
                producer.sendRecord(topic.toByteArray(), recordId(collectionNumber, messageNumber), body(messageNumber), timestamp)
                logger.info("Sent record $messageNumber/$RECORDS_PER_TOPIC to kafka topic '$topic'.")
            }
        }
        logger.info("Started record producer")
    }

    private suspend fun verifyHbase() {
        var waitSoFarSecs = 0
        val shortInterval = 5
        val longInterval = 10
        val expectedTablesSorted = expectedTables.sorted()
        logger.info("Waiting for ${expectedTablesSorted.size} hbase tables to appear; Expecting to create: $expectedTablesSorted")
        HbaseClient.connect().use { hbase ->
            withTimeout(30.minutes) {
                do {
                    val foundTablesSorted = loadTestTables(hbase)
                    logger.info("Waiting for ${expectedTablesSorted.size} hbase tables to appear; Found ${foundTablesSorted.size}; Total of $waitSoFarSecs seconds elapsed")
                    delay(longInterval.seconds)
                    waitSoFarSecs += longInterval
                } while (expectedTablesSorted.toSet() != foundTablesSorted.toSet())


                loadTestTables(hbase).forEach { tableName ->
                    launch (Dispatchers.IO) {
                        hbase.connection.getTable(TableName.valueOf(tableName)).use { table ->
                            do {
                                val foundRecords = recordCount(table)
                                logger.info("Waiting for $RECORDS_PER_TOPIC hbase records to appear in $tableName; Found $foundRecords; Total of $waitSoFarSecs seconds elapsed")
                                delay(shortInterval.seconds)
                                waitSoFarSecs += shortInterval
                            } while (foundRecords < RECORDS_PER_TOPIC)
                        }
                    }
                }
            }
        }
    }

    private fun verifyS3() {
        val contentsList = allArchiveObjectContentsAsJson()
        contentsList.size shouldBe TOPIC_COUNT * RECORDS_PER_TOPIC
        contentsList.forEach {
            it["traceId"].asJsonPrimitive.asString shouldBe "00002222-abcd-4567-1234-1234567890ab"
            val message = it["message"]
            message shouldNotBe null
            message!!.asJsonObject shouldNotBe null
            message.asJsonObject!!["dbObject"] shouldNotBe null
            it["message"]!!.asJsonObject!!["dbObject"]!!.asJsonPrimitive shouldNotBe null
            it["message"]!!.asJsonObject!!["dbObject"]!!.asJsonPrimitive!!.asString shouldBe "bubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9A"
        }
    }

    private fun verifyManifests() {
        val contentsList = allManifestObjectContentsAsString()
        contentsList.forEach {
            val manifestLines = it.split("\n")
            manifestLines.filter(String::isNotBlank).forEach {
                val fields = it.split("|")
                fields.size shouldBe 8
                fields.get(0) shouldNotBe ""
                fields.get(1) shouldNotBe ""
                fields.get(2) shouldNotBe ""
                fields.get(3) shouldNotBe ""
                fields.get(4) shouldBe "STREAMED"
                fields.get(5) shouldBe "K2HB"
                fields.get(6) shouldNotBe ""
                fields.get(7) shouldBe "KAFKA_RECORD"
            }
        }
    }

    private fun allArchiveObjectContentsAsJson(): List<JsonObject> =
            objectSummaries(Config.ArchiveS3.archiveBucket, Config.ArchiveS3.archiveDirectory, ArchiveAwsS3Service.s3)
                .filter { it.key.endsWith("jsonl.gz") && it.key.contains("load_test") }
                .map(S3ObjectSummary::getKey)
                .map(this@Kafka2hbIntegrationLoadSpec::archiveObjectContents)
                .map(::String)
                .flatMap { it.split("\n") }
                .filter(String::isNotEmpty)
                .map { Gson().fromJson(it, JsonObject::class.java) }

    private fun archiveObjectContents(key: String) =
            GZIPInputStream(ArchiveAwsS3Service.s3.getObject(GetObjectRequest(Config.ArchiveS3.archiveBucket, key)).objectContent).use {
                ByteArrayOutputStream().also { output -> it.copyTo(output) }
            }.toByteArray()

    private fun allManifestObjectContentsAsString(): List<String> =
            objectSummaries(Config.ManifestS3.manifestBucket, Config.ManifestS3.manifestDirectory, ManifestAwsS3Service.s3)
                .filter { it.key.endsWith("txt") && it.key.contains("load-test") }
                .map(S3ObjectSummary::getKey)
                .map(this@Kafka2hbIntegrationLoadSpec::manifestObjectContents)
                .map(::String)

    private fun manifestObjectContents(key: String) =
            ManifestAwsS3Service.s3.getObject(GetObjectRequest(Config.ManifestS3.manifestBucket, key))
                .objectContent.use {
                    ByteArrayOutputStream().also { output -> it.copyTo(output) }
                }.toByteArray()

    private fun objectSummaries(s3BucketName: String, s3Prefix: String, s3Connection: AmazonS3): MutableList<S3ObjectSummary> {
        val objectSummaries = mutableListOf<S3ObjectSummary>()
        val request = ListObjectsV2Request().apply {
            bucketName = s3BucketName
            prefix = s3Prefix
        }

        var objectListing: ListObjectsV2Result?

        do {
            objectListing = s3Connection.listObjectsV2(request)
            objectSummaries.addAll(objectListing.objectSummaries)
            request.continuationToken = objectListing.nextContinuationToken
        } while (objectListing != null && objectListing.isTruncated)

        return objectSummaries
    }

    private fun recordCount(table: Table) = table.getScanner(Scan()).count()
    private val expectedTables by lazy { (0..9).map { tableName(it) } }

    private fun loadTestTables(hbase: HbaseClient): List<String> {
        val tables = hbase.connection.admin.listTableNames()
            .map(TableName::getNameAsString)
            .filter(Regex(tableNamePattern())::matches)
            .sorted()
        logger.info("...hbase tables: found ${tables.size}: $tables")
        return tables
    }

    private fun tableName(counter: Int) = sampleQualifiedTableName("$DB_NAME$counter", "$COLLECTION_NAME$counter")
    private fun tableNamePattern() = """$DB_NAME\d+:$COLLECTION_NAME\d+""".replace("-", "_").replace(".", "_")

    private fun topicName(collectionNumber: Int)
            = "db.$DB_NAME$collectionNumber.$COLLECTION_NAME$collectionNumber"

    private fun recordId(collectionNumber: Int, messageNumber: Int) =
            "key-$messageNumber/$collectionNumber".toByteArray()

    private fun body(recordNumber: Int) = """{
        "traceId": "00002222-abcd-4567-1234-1234567890ab",
        "unitOfWorkId": "00002222-abcd-4567-1234-1234567890ab",
        "@type": "V4",
        "version": "core-X.release_XXX.XX",
        "timestamp": "2018-12-14T15:01:02.000+0000",
        "message": {
            "@type": "MONGO_UPDATE",
            "collection": "$COLLECTION_NAME",
            "db": "$DB_NAME",
            "_id": {
                "id": "$DB_NAME/$COLLECTION_NAME/$recordNumber"
            },
            "_lastModifiedDateTime": "${getISO8601Timestamp()}",
            "encryption": {
                "encryptionKeyId": "cloudhsm:1,2",
                "encryptedEncryptionKey": "bHJjhg2Jb0uyidkl867gtFkjl4fgh9Ab",
                "initialisationVector": "kjGyvY67jhJHVdo2",
                "keyEncryptionKeyId": "cloudhsm:1,2"
            },
            "dbObject": "bubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9A",
            "timestamp_created_from": "_lastModifiedDateTime"
        }
    }""".toByteArray()
}


