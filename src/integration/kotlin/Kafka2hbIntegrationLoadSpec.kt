import com.amazonaws.services.s3.model.*
import com.google.gson.Gson
import com.google.gson.JsonObject
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.*
import lib.getISO8601Timestamp
import lib.sampleQualifiedTableName
import lib.sendRecord
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
import lib.*

@ExperimentalTime
class Kafka2hbIntegrationLoadSpec : StringSpec() {

    companion object {
        private val log = Logger.getLogger(Kafka2hbIntegrationLoadSpec::class.toString())
        private const val TOPIC_COUNT = 10
        private const val RECORDS_PER_TOPIC = 1_000
        private const val DB_NAME = "load-test-database"
        private const val COLLECTION_NAME = "load-test-collection"
    }

    init {
        "Many messages sent to many topics" {
            publishRecords()
            verifyHbase()
            verifyMetadataStore(TOPIC_COUNT * RECORDS_PER_TOPIC, DB_NAME, false)
            verifyS3()
        }
    }

    private fun publishRecords() {
        val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.producerProps)
        val converter = Converter()
        println("Starting record producer...")
        repeat(TOPIC_COUNT) { collectionNumber ->
            val topic = topicName(collectionNumber)
            repeat(RECORDS_PER_TOPIC) { messageNumber ->
                val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())
                log.info("Sending record $messageNumber/$RECORDS_PER_TOPIC to kafka topic '$topic'.")
                producer.sendRecord(topic.toByteArray(), recordId(collectionNumber, messageNumber), body(messageNumber), timestamp)
                log.info("Sent record $messageNumber/$RECORDS_PER_TOPIC to kafka topic '$topic'.")
            }
        }
        println("...Started record producer")
    }

    private suspend fun verifyHbase() {
        var waitSoFarSecs = 0
        val shortInterval = 5
        val longInterval = 10
        val expectedTablesSorted = expectedTables.sorted()
        println("Waiting for ${expectedTablesSorted.size} hbase tables to appear; Expecting to create: $expectedTablesSorted")
        HbaseClient.connect().use { hbase ->
            withTimeout(30.minutes) {
                do {
                    val foundTablesSorted = loadTestTables(hbase)
                    println("Waiting for ${expectedTablesSorted.size} hbase tables to appear; Found ${foundTablesSorted.size}; Total of $waitSoFarSecs seconds elapsed")
                    delay(longInterval.seconds)
                    waitSoFarSecs += longInterval
                } while (expectedTablesSorted.toSet() != foundTablesSorted.toSet())

                loadTestTables(hbase).forEach { tableName ->
                    hbase.connection.getTable(TableName.valueOf(tableName)).use { table ->
                        do {
                            val foundRecords = recordCount(table)
                            println("Waiting for $RECORDS_PER_TOPIC hbase records to appear in $tableName; Found $foundRecords; Total of $waitSoFarSecs seconds elapsed")
                            delay(shortInterval.seconds)
                            waitSoFarSecs += shortInterval
                        } while (foundRecords < RECORDS_PER_TOPIC)
                    }
                }
            }
        }
    }

    private fun verifyS3() {
        val contentsList = allObjectContentsAsJson()
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

    private fun allObjectContentsAsJson(): List<JsonObject> =
            objectSummaries()
                .filter { it.key.endsWith("jsonl.gz") && it.key.contains("load_test") }
                .map { it.key }
                .map { objectContents(it) }
                .map { String(it) }
                .flatMap { it.split("\n") }
                .filter { it.isNotEmpty() }
                .map { Gson().fromJson(it, JsonObject::class.java) }

    private fun objectContents(key: String) =
            GZIPInputStream(AwsS3Service.s3.getObject(GetObjectRequest(Config.AwsS3.archiveBucket, key)).objectContent).use {
                ByteArrayOutputStream().also { output -> it.copyTo(output) }
            }.toByteArray()

    private fun objectSummaries(): MutableList<S3ObjectSummary> {
        val objectSummaries = mutableListOf<S3ObjectSummary>()
        val request = ListObjectsV2Request().apply {
            bucketName = Config.AwsS3.archiveBucket
            prefix = Config.AwsS3.archiveDirectory
        }

        var objectListing: ListObjectsV2Result?

        do {
            objectListing = AwsS3Service.s3.listObjectsV2(request)
            objectSummaries.addAll(objectListing.objectSummaries)
            request.continuationToken = objectListing.nextContinuationToken
        } while (objectListing != null && objectListing.isTruncated)

        return objectSummaries
    }

    private fun recordCount(table: Table) = table.getScanner(Scan()).count()
    private val expectedTables by lazy { (0..9).map { tableName(it) } }

    private fun loadTestTables(hbase: HbaseClient): List<String> {
        val tables = hbase.connection.admin.listTableNames()
            .map { it.nameAsString }
            .filter { Regex(tableNamePattern()).matches(it) }
            .sorted()
        println("...hbase tables: found ${tables.size}: $tables")
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


