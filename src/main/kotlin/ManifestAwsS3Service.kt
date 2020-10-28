
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.beust.klaxon.JsonObject
import com.beust.klaxon.KlaxonException
import com.beust.klaxon.Parser
import org.apache.commons.text.StringEscapeUtils
import java.io.BufferedOutputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util.*
import kotlin.system.measureTimeMillis

open class ManifestAwsS3Service(private val amazonS3: AmazonS3) {

    open suspend fun putManifestFile(payloads: List<HbasePayload>) {
        if (payloads.isNotEmpty()) {
            val prefix = Config.ManifestS3.manifestDirectory
            val fileName = manifestFileName(payloads)
            val key = "${prefix}/${fileName}"
            logger.info("Putting manifest into s3", "size", "${payloads.size}", "key", key)
            val timeTaken = measureTimeMillis { putManifest(key, manifestBody(payloads)) }
            logger.info("Put manifest into s3", "time_taken", "$timeTaken", "size", "${payloads.size}", "key", key)
        }
    }

    private fun putManifest(key: String, body: ByteArray) =
        amazonS3.putObject(PutObjectRequest(Config.ManifestS3.manifestBucket, key,
                ByteArrayInputStream(body), objectMetadata(body)))

    private fun manifestBody(payloads: List<HbasePayload>) =
        ByteArrayOutputStream().also {
            BufferedOutputStream(it).use { bufferedOutputStream ->
                payloads.forEach { payload ->
                    manifestRecordForPayload(payload)?.let { manifestRecord ->
                        bufferedOutputStream.write(csv(manifestRecord).toByteArray(Charset.forName("UTF-8")))
                    }
                }
            }
        }.toByteArray()

    private fun manifestRecordForPayload(payload: HbasePayload): ManifestRecord? {
        return textUtils.topicNameTableMatcher(payload.record.topic())?.let {
            val (database, collection) = it.destructured
            ManifestRecord(stripId(payload.id), payload.version, database, collection,
                    MANIFEST_RECORD_SOURCE, MANIFEST_RECORD_COMPONENT, MANIFEST_RECORD_TYPE, payload.id)
        }
    }

    private fun stripId(id: String): String {
        try {
            val parser: Parser = Parser.default()
            val stringBuilder: StringBuilder = StringBuilder(id)
            val json = parser.parse(stringBuilder) as JsonObject
            val idValue = json["id"]
            return if (idValue != null) {
                when (idValue) {
                    is String -> {
                        idValue
                    }
                    is Int -> {
                        "$idValue"
                    }
                    else -> {
                        id
                    }
                }
            } else {
                id
            }
        } catch (e: KlaxonException) {
            return id
        }
    }

    private fun manifestFileName(payloads: List<HbasePayload>): String {
        val firstRecord = payloads.first().record
        val firstPartition = firstRecord.partition()
        val firstOffset = firstRecord.offset()
        val firstTopic = firstRecord.topic()

        val lastRecord = payloads.last().record
        val lastPartition = lastRecord.partition()
        val lastOffset =  lastRecord.offset()
        val lastTopic = lastRecord.topic()

        return "${firstTopic}_${firstPartition}_$firstOffset-${lastTopic}_${lastPartition}_$lastOffset.txt"
    }

    private fun objectMetadata(body: ByteArray)
        = ObjectMetadata().apply {
            contentLength = body.size.toLong()
            contentType = "application/text"

            addUserMetadata("batch_receipt_time", SimpleDateFormat("yyyy/MM/dd HH:mm:ss").apply {
                timeZone = TimeZone.getTimeZone("UTC")
            }.format(Date()))
        }

    private fun csv(manifestRecord: ManifestRecord) =
            "${escape(manifestRecord.id)}|${escape(manifestRecord.timestamp.toString())}|${escape(manifestRecord.db.replace('_', '-'))}|${escape(manifestRecord.collection)}|${escape(manifestRecord.source)}|${escape(manifestRecord.externalOuterSource)}|${escape(manifestRecord.originalId)}|${escape(manifestRecord.externalInnerSource)}\n"

    private fun escape(value: String) = StringEscapeUtils.escapeCsv(value)

    companion object {
        fun connect() = ManifestAwsS3Service(s3)
        val textUtils = TextUtils()
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(ManifestAwsS3Service::class.toString())
        val s3 = Config.AwsS3.s3
        const val MANIFEST_RECORD_SOURCE = "STREAMED"
        const val MANIFEST_RECORD_COMPONENT = "K2HB"
        const val MANIFEST_RECORD_TYPE = "KAFKA_RECORD"
    }
}
