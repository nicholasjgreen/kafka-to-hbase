
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.PutObjectRequest
import com.nhaarman.mockitokotlin2.*
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.io.InputStreamReader
import java.io.LineNumberReader
import java.text.SimpleDateFormat
import java.util.*
import java.util.zip.GZIPInputStream

class ManifestAwsS3ServiceTest : StringSpec() {
    init {
        "Put manifest file set request parameters correctly" {
            val amazonS3 = mock<AmazonS3>()
            val manifestAwsS3Service = ManifestAwsS3Service(amazonS3)
            val payloads = hbasePayloads()
            manifestAwsS3Service.putManifestFile("database_one:collection_one", payloads)
            val requestCaptor = argumentCaptor<PutObjectRequest>()
            verify(amazonS3, times(1)).putObject(requestCaptor.capture())
            verifyNoMoreInteractions(amazonS3)
            val request = requestCaptor.firstValue
            request.bucketName shouldBe "manifests"
            request.key shouldBe "streaming/${today()}/db.database-one.collection_one_10_1-100.txt"
            val lineReader = LineNumberReader(InputStreamReader(request.inputStream))

            var lineCount = 0
            lineReader.forEachLine {
                it shouldBe manifestBody(lineReader.lineNumber).replace('\n', ' ')
                lineCount++
            }
            lineCount shouldBe payloads.count()
        }

        "Put manifest file set user metadata correctly" {
            val amazonS3 = mock<AmazonS3>()
            val manifestAwsS3Service = ManifestAwsS3Service(amazonS3)
            val payloads = hbasePayloads()
            manifestAwsS3Service.putManifestFile("database_one:collection_one", payloads)
            val requestCaptor = argumentCaptor<PutObjectRequest>()
            verify(amazonS3, times(1)).putObject(requestCaptor.capture())
            verifyNoMoreInteractions(amazonS3)
            val request = requestCaptor.firstValue
            validateUserMetadata(request.metadata.userMetadata)
        }
    }

    private fun hbasePayloads(): List<HbasePayload>
            = (1..100).map { index ->
                val consumerRecord = mock<ConsumerRecord<ByteArray, ByteArray>> {
                    on { key() } doReturn index.toString().toByteArray()
                    on { topic() } doReturn "db.database-one.collection_one"
                    on { offset() } doReturn index.toLong()
                    on { partition() } doReturn 10
                }
                if(index % 2 == 0) {
                    HbasePayload(Bytes.toBytes("key-$index"), messageBody(index).toByteArray(), "{\"id\":\"id-$index\"}", payloadTime(index), consumerRecord)
                } else {
                    HbasePayload(Bytes.toBytes("key-$index"), messageBody(index).toByteArray(), "id-$index", payloadTime(index), consumerRecord)
                }
            }

    private fun messageBody(index: Int) =
        """
        {
            "message": {
                "dbObject": "abcdefghijklmnopqrstuvwxyz" 
            },
            "position": $index 
        }
        """.trimIndent()

    private fun manifestBody(index: Int) =
        if(index % 2 == 0) {
            """
            id-$index|${payloadTime(index)}|database-one|collection_one|STREAMED|K2HB|"{""id"":""id-$index""}"|KAFKA_RECORD
            """.trimIndent()
        } else {
            """
            id-$index|${payloadTime(index)}|database-one|collection_one|STREAMED|K2HB|id-$index|KAFKA_RECORD
            """.trimIndent()
        }

    private fun validateUserMetadata(userMetadata: MutableMap<String, String>) {
        userMetadata["database"] shouldBe "database-one"
        userMetadata["collection"] shouldBe "collection_one"
    }

    private fun today() = dateFormat().format(Date())
    private fun payloadTime(index: Int) = payloadTimestamp(index).time
    private fun payloadTimestamp(index: Int) = dateFormat().parse(payloadDate(index))
    private fun dateFormat() = SimpleDateFormat("yyyy/MM/dd").apply { timeZone = TimeZone.getTimeZone("UTC") }
    private fun payloadDate(index: Int) = "2020/01/%02d".format((index % 20) + 1)
}
