import com.beust.klaxon.JsonObject
import com.beust.klaxon.KlaxonException
import com.beust.klaxon.Parser
import com.beust.klaxon.lookup
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.*
import java.util.zip.CRC32

open class Converter {
    open fun convertToJson(body: ByteArray): JsonObject {
        try {
            val parser: Parser = Parser.default()
            val stringBuilder: StringBuilder = StringBuilder(String(body))
            return parser.parse(stringBuilder) as JsonObject
        } catch (e: KlaxonException) {
            logger.warn("Error while parsing json", "cause", e.message?:"")
            throw IllegalArgumentException("Cannot parse invalid JSON")
        }
    }

    fun sortJsonByKey(unsortedJson: JsonObject): String {
        val sortedEntries = unsortedJson.toSortedMap(compareBy { it })
        val json: JsonObject = JsonObject(sortedEntries)
        return json.toJsonString()
    }

    fun generateFourByteChecksum(input: String): ByteArray {
        val bytes = input.toByteArray()
        val checksum = CRC32()

        checksum.update(bytes, 0, bytes.size)

        return ByteBuffer.allocate(4).putInt(checksum.value.toInt()).array()
    }

    fun encodeToBase64(input: String): String {
        return Base64.getEncoder().encodeToString(input.toByteArray())
    }

    fun decodeFromBase64(input: String): String {
        val decodedBytes: ByteArray = Base64.getDecoder().decode(input)
        return String(decodedBytes)
    }

    open fun getTimestampAsLong(timeStampAsStr: String?, timeStampPattern: String = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ"): Long {
        val df = SimpleDateFormat(timeStampPattern)
        return df.parse(timeStampAsStr).time
    }

    open fun getLastModifiedTimestamp(json: JsonObject?): String? {
        val epoch = "1980-01-01T00:00:00.000Z"
        
        val lastModifiedTimestampStr = json?.lookup<String?>("message._lastModifiedDateTime")?.get(0)
        if (!lastModifiedTimestampStr.isNullOrBlank()) {
            return lastModifiedTimestampStr
        }
        
        val createdTimestampStr = json?.lookup<String?>("message.createdDateTime")?.get(0)
        if (!createdTimestampStr.isNullOrBlank()) {
            return createdTimestampStr
        }

        return epoch
    }

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(Converter::class.toString())
    }
}
