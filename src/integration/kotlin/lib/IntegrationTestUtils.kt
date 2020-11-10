package lib

import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import io.kotest.matchers.longs.shouldBeGreaterThanOrEqual
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.apache.hadoop.hbase.TableName
import java.sql.Connection
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.minutes
import kotlin.time.seconds

fun getId() = """{ "exampleId": "aaaa1111-abcd-4567-1234-1234567890ab"}"""

fun getEqualityId() = """{ "messageId": "aaaa1111-abcd-4567-4321-1234567890ab"}"""

fun wellFormedValidPayload(
    dbName: String = "exampleDbName",
    collectionName: String = "exampleCollectionName"
) = """{
        "traceId": "00001111-abcd-4567-1234-1234567890ab",
        "unitOfWorkId": "00002222-abcd-4567-1234-1234567890ab",
        "@type": "V4",
        "version": "core-X.release_XXX.XX",
        "timestamp": "2018-12-14T15:01:02.000+0000",
        "message": {
            "@type": "MONGO_UPDATE",
            "collection": "$collectionName",
            "db": "$dbName",
            "_id": ${getId()},
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

fun wellFormedValidPayloadEquality() = """{
        "traceId": "00001111-abcd-4567-4321-1234567890ab",
        "unitOfWorkId": "00002222-abcd-7654-1234-1234567890ab",
		"@type": "V4",
		"version": "core-X.release_XXX.X",
		"timestamp": "2020-05-21T17:18:15.706+0000",
		"message": {
			"@type": "EQUALITY_QUESTIONS_ANSWERED",
            "_id": ${getEqualityId()},
			"_lastModifiedDateTime": "${getISO8601Timestamp()}",
			"encryption": {
				"keyEncryptionKeyId": "cloudhsm:1,2",
				"encryptedEncryptionKey": "bHJjhg2Jb0uyidkl867gtFkjl4fgh9Ab",
				"initialisationVector": "kjGyvY67jhJHVdo2"
			},
			"dbObject": "xxxxxx",
            "timestamp_created_from": "_lastModifiedDateTime"
		}
	}""".toByteArray()

fun getISO8601Timestamp(): String {
    val tz = TimeZone.getTimeZone("UTC")
    val df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ")
    df.timeZone = tz
    return df.format(Date())
}

fun uniqueTopicName() = "db.database.collection_${Instant.now().toEpochMilli()}"

fun uniqueTopicNameWithDot() = "db.database.collec.tion_${Instant.now().toEpochMilli()}"

fun uniqueEqualityTopicName() = "data.equality_${Instant.now().toEpochMilli()}"

fun sampleQualifiedTableName(namespace: String, tableName: String) =
    "$namespace:$tableName".replace("-", "_").replace(".", "_")

fun getS3Client(): AmazonS3 {
    return AmazonS3ClientBuilder.standard()
        .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration("http://aws-s3:4566", "eu-west-2"))
        .withClientConfiguration(ClientConfiguration().withProtocol(Protocol.HTTP))
        .withCredentials(
            AWSStaticCredentialsProvider(BasicAWSCredentials("aws-access-key", "aws-secret-access-key"))
        )
        .withPathStyleAccessEnabled(true)
        .disableChunkedEncoding()
        .build()
}

fun metadataStoreConnection(): Connection {
    val (url, properties) = MetadataStoreClient.connectionProperties()
    return DriverManager.getConnection(url, properties)
}

fun verifyMetadataStore(expectedCount: Int, expectedTopicName: String, exactMatch: Boolean = true) =
    metadataStoreConnection().use { connection ->
        connection.createStatement().use { statement ->
            val results =
                statement.executeQuery("SELECT count(*) FROM ucfs WHERE topic_name like '%$expectedTopicName%'")
            results.next() shouldBe true
            val count = results.getLong(1)
            if (exactMatch) {
                count shouldBe expectedCount.toLong()
            } else {
                count shouldBeGreaterThanOrEqual expectedCount.toLong()
            }
        }
    }

@ExperimentalTime
suspend fun verifyHbaseRegions(expectedTablesToRegions: Int, regionReplication: Int, regionServers: Int) {

    var waitSoFarSecs = 0
    val longInterval = 5

    val foundTablesToRegions = mutableMapOf<String, Int>()

    HbaseClient.connect().use { hbase ->
        withTimeout(10.minutes) {
            do {
                val foundTablesSorted = testTables()
                delay(longInterval.seconds)
                waitSoFarSecs += longInterval
            }
            while (expectedTablesToRegions != foundTablesSorted.size)

            testTables().forEach { tableName ->
                launch(Dispatchers.IO) {
                    val regionsWithReplication = hbase.getTableRegions(TableName.valueOf(tableName)).size
                    foundTablesToRegions[tableName] = regionsWithReplication
                }
            }
        }
    }
    foundTablesToRegions.values.contains(regionReplication * regionServers) shouldBe true
    foundTablesToRegions.values.toSet().size shouldBe 1
}

fun testTables(): MutableSet<String> = HbaseClient.connect().tables.keys
