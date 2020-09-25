
import Config.AwsS3.localstackAccessKey
import Config.AwsS3.localstackSecretKey
import Config.AwsS3.localstackServiceEndPoint
import Config.dataworksRegion
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.commons.codec.binary.Hex
import java.io.BufferedOutputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util.*
import java.util.zip.GZIPOutputStream
import kotlin.system.measureTimeMillis

open class ArchiveAwsS3Service(private val amazonS3: AmazonS3) {

    open suspend fun putObjectsAsBatch(hbaseTable: String, payloads: List<HbasePayload>) {
        if (payloads.isNotEmpty()) {
            val (database, collection) = hbaseTable.split(Regex(":"))
            val key = batchKey(database, collection, payloads)
            logger.info("Putting batch into s3", "size", "${payloads.size}", "hbase_table", hbaseTable, "key", key)
            val timeTaken = measureTimeMillis { putBatchObject(key, batchBody(payloads)) }
            logger.info("Put batch into s3", "time_taken", "$timeTaken", "size", "${payloads.size}", "hbase_table", hbaseTable, "key", key)
        }
    }

    open suspend fun putObjects(hbaseTable: String, payloads: List<HbasePayload>) {
        logger.info("Putting batch into s3", "size", "${payloads.size}", "hbase_table", hbaseTable)
        val timeTaken = measureTimeMillis {
            val (database, collection) = hbaseTable.split(Regex(":"))
            coroutineScope {
                payloads.forEach { payload ->
                    if (Config.ArchiveS3.parallelPuts) {
                        launch { putPayload(database, collection, payload) }
                    } else {
                        putPayload(database, collection, payload)
                    }
                }
            }
        }
        logger.info("Put batch into s3", "time_taken", "$timeTaken", "size", "${payloads.size}", "hbase_table", hbaseTable)
    }

    private fun putBatchObject(key: String, body: ByteArray) =
        amazonS3.putObject(PutObjectRequest(Config.ArchiveS3.archiveBucket, key,
                ByteArrayInputStream(body), ObjectMetadata().apply {
            contentLength = body.size.toLong()
        }))


    private fun batchBody(payloads: List<HbasePayload>) =
        ByteArrayOutputStream().also {
            BufferedOutputStream(GZIPOutputStream(it)).use { bufferedOutputStream ->
                payloads.forEach { payload ->
                    val body = StringBuilder(String(payload.body, Charset.forName("UTF-8"))
                            .replace("\n", " ")).append('\n')
                    bufferedOutputStream.write(body.toString().toByteArray(Charset.forName("UTF-8")))
                }
            }
        }.toByteArray()

    private suspend fun putPayload(database: String, collection: String, payload: HbasePayload)
            = withContext(Dispatchers.IO) {
                val hexedId = Hex.encodeHexString(payload.key)
                launch { putObject(archiveKey(database, collection, hexedId, payload.version), payload, database, collection) }
                launch { putObject(latestKey(database, collection, hexedId), payload, database, collection)}
            }


    private suspend fun putObject(key: String, payload: HbasePayload, database: String, collection: String)
            = withContext(Dispatchers.IO) { amazonS3.putObject(putObjectRequest(key, payload, database, collection)) }

    private fun batchKey(database: String, collection: String, payloads: List<HbasePayload>): String {
        val firstRecord = payloads.first().record
        val last = payloads.last().record
        val partition = firstRecord.partition()
        val firstOffset = firstRecord.offset()
        val lastOffset =  last.offset()
        val topic = firstRecord.topic()
        val filename = "${topic}_${partition}_$firstOffset-$lastOffset"
        return "${Config.ArchiveS3.archiveDirectory}/${simpleDateFormatter().format(Date())}/$database/$collection/$filename.jsonl.gz"
    }

    // K2HB_S3_LATEST_PATH: s3://data_bucket/ucdata_main/latest/<db>/<collection>/<id-hex>.json
    private fun latestKey(database: String, collection: String, hexedId: String) =
            "${Config.ArchiveS3.archiveDirectory}/latest/$database/$collection/$hexedId.json"

    // K2HB_S3_TIMESTAMPED_PATH: s3://data_bucket/ucdata_main/<yyyy>/<mm>/<dd>/<db>/<collection>/<id-hex>/<timestamp>.json
    private fun archiveKey(database: String, collection: String, hexedId: String, version: Long)
            = "${Config.ArchiveS3.archiveDirectory}/${datePath(version)}/$database/$collection/$hexedId/${version}.json"

    private fun datePath(version: Long) = simpleDateFormatter().format(version)

    private fun putObjectRequest(key: String, payload: HbasePayload, database: String, collection: String) =
            PutObjectRequest(Config.ArchiveS3.archiveBucket,
                    key, ByteArrayInputStream(payload.body), objectMetadata(payload, database, collection))

    private fun objectMetadata(payload: HbasePayload, database: String, collection: String)
        = ObjectMetadata().apply {
            contentLength = payload.body.size.toLong()
            contentType = "application/json"
            addUserMetadata("kafka_message_id", String(payload.record.key()))

            addUserMetadata("receipt_time", SimpleDateFormat("yyyy/MM/dd HH:mm:ss").apply {
                timeZone = TimeZone.getTimeZone("UTC")
            }.format(Date()))

            addUserMetadata("hbase_id", textUtils.printableKey(payload.key))
            addUserMetadata("database", database.replace('_', '-'))
            addUserMetadata("collection", collection.replace('_', '-'))
            addUserMetadata("id", String(payload.key).substring(4))
            addUserMetadata("timestamp", payload.version.toString())
        }


    private fun simpleDateFormatter() = SimpleDateFormat("yyyy/MM/dd").apply { timeZone = TimeZone.getTimeZone("UTC") }

    companion object {
        fun connect() = ArchiveAwsS3Service(s3)
        val textUtils = TextUtils()
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(ArchiveAwsS3Service::class.toString())
        val s3 = Config.AwsS3.s3
    }
}
