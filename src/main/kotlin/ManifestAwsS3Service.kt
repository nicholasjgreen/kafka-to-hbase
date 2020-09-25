
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
import org.apache.commons.text.StringEscapeUtils
import com.beust.klaxon.JsonObject

open class ManifestAwsS3Service(private val amazonS3: AmazonS3) {

    open suspend fun putManifestFile(hbaseTable: String, payloads: List<HbasePayload>) {
        if (payloads.isNotEmpty()) {
            val (database, collection) = hbaseTable.split(Regex(":"))
            val prefix = dateStampedPrefix()
            val fileName = manifestFileName(payloads)
            val key = "${prefix}/${fileName}"
            logger.info("Putting manifest into s3", "size", "${payloads.size}", "hbase_table", hbaseTable, "key", key)
            val timeTaken = measureTimeMillis { putManifest(database, collection, key, manifestBody(database, collection, payloads)) }
            logger.info("Put manifest into s3", "time_taken", "$timeTaken", "size", "${payloads.size}", "hbase_table", hbaseTable, "key", key)
        }
    }

    private fun putManifest(database: String, collection: String, key: String, body: ByteArray) =
        amazonS3.putObject(PutObjectRequest(Config.ManifestS3.manifestBucket, key,
                ByteArrayInputStream(body), objectMetadata(body, database, collection)))

    private fun manifestBody(database: String, collection: String, payloads: List<HbasePayload>) =
        ByteArrayOutputStream().also {
            BufferedOutputStream(it).use { bufferedOutputStream ->
                payloads.forEach { payload ->
                    val manifestRecord = manifestRecordForPayload(database, collection, payload)
                    val body = csv(manifestRecord)
                    bufferedOutputStream.write(body.toString().toByteArray(Charset.forName("UTF-8")))
                }
            }
        }.toByteArray()

    private fun manifestRecordForPayload(database: String, collection: String, payload: HbasePayload): ManifestRecord
            = ManifestRecord(payload.id, payload.version, database, collection, 
                MANIFEST_RECORD_SOURCE, MANIFEST_RECORD_COMPONENT, MANIFEST_RECORD_TYPE, payload.id)

    // K2HB_MANIFEST_FILE_PATH: s3://manifest/streaming/<yyyy>/<mm>/<dd>/<db>_<collection>_<uniqueid>.json
    private fun dateStampedPrefix()
            = "${Config.ManifestS3.manifestDirectory}/${dateNowPath()}"

    private fun manifestFileName(payloads: List<HbasePayload>): String {
        val firstRecord = payloads.first().record
        val last = payloads.last().record
        val partition = firstRecord.partition()
        val firstOffset = firstRecord.offset()
        val lastOffset =  last.offset()
        val topic = firstRecord.topic()
        return "${topic}_${partition}_$firstOffset-$lastOffset.csv"
    }
    
    private fun dateNowPath() = simpleDateFormatter().format(Calendar.getInstance().getTime())

    private fun objectMetadata(body: ByteArray, database: String, collection: String)
        = ObjectMetadata().apply {
            contentLength = body.size.toLong()
            contentType = "application/text"

            addUserMetadata("batch_receipt_time", SimpleDateFormat("yyyy/MM/dd HH:mm:ss").apply {
                timeZone = TimeZone.getTimeZone("UTC")
            }.format(Date()))

            addUserMetadata("database", database.replace('_', '-'))
            addUserMetadata("collection", collection)
        }

    private fun csv(manifestRecord: ManifestRecord) =
            "${escape(manifestRecord.id)}|${escape(manifestRecord.timestamp.toString())}|${escape(manifestRecord.db.replace('_', '-'))}|${escape(manifestRecord.collection)}|${escape(manifestRecord.source)}|${escape(manifestRecord.externalOuterSource)}|${escape(manifestRecord.originalId)}|${escape(manifestRecord.externalInnerSource)}\n"

    private fun escape(value: String) = StringEscapeUtils.escapeCsv(value)

    private fun simpleDateFormatter() = SimpleDateFormat("yyyy/MM/dd").apply { timeZone = TimeZone.getTimeZone("UTC") }

    companion object {
        fun connect() = ManifestAwsS3Service(s3)
        val textUtils = TextUtils()
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(ManifestAwsS3Service::class.toString())
        val s3 = Config.AwsS3.s3
        val MANIFEST_RECORD_SOURCE = "STREAMED"
        val MANIFEST_RECORD_COMPONENT = "K2HB"
        val MANIFEST_RECORD_TYPE = "KAFKA_RECORD"
    }
}