import com.beust.klaxon.JsonObject
import com.beust.klaxon.KlaxonException
import com.beust.klaxon.Parser
import com.beust.klaxon.lookup
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.*
import java.util.logging.Logger
import java.util.zip.CRC32

open class Converter() {
    private val log: Logger = Logger.getLogger("Converter")

    open fun convertToJson(body: ByteArray): JsonObject {

        try {
            val parser: Parser = Parser.default()
            val stringBuilder: StringBuilder = StringBuilder(String(body))
            val json: JsonObject = parser.parse(stringBuilder) as JsonObject
            return json
        } catch (e: KlaxonException) {
            log.warning(
                "Error while parsing message in to json: ${e.toString()}"
            )
            throw IllegalArgumentException("Cannot parse invalid JSON")
        }
    }

    fun sortJsonByKey(unsortedJson: JsonObject): String {
        val sortedEntries = unsortedJson.toSortedMap(compareBy<String> { it })
        val json: JsonObject = JsonObject(sortedEntries)

        return json.toJsonString()
    }

    fun generateFourByteChecksum(input: String): ByteArray {
        val bytes = input.toByteArray()
        val checksum = CRC32()

        checksum.update(bytes, 0, bytes.size)

        return ByteBuffer.allocate(4).putInt(checksum.getValue().toInt()).array();
    }

    fun encodeToBase64(input: String): String {
        return Base64.getEncoder().encodeToString(input.toByteArray());
    }

    fun decodeFromBase64(input: String): String {
        val decodedBytes: ByteArray = Base64.getDecoder().decode(input);
        return String(decodedBytes);
    }

    open fun getTimestampAsLong(timeStampAsStr: String?, timeStampPattern: String = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ"): Long {
        val df = SimpleDateFormat(timeStampPattern);
        return df.parse(timeStampAsStr).time
    }

    open fun getLastModifiedTimestamp(json: JsonObject?): String? {
        val lastModifiedTimestampStr = json?.lookup<String?>("message._lastModifiedDateTime")?.get(0)
        if (lastModifiedTimestampStr.isNullOrBlank()) throw RuntimeException("Last modified date time is null or blank")
        return lastModifiedTimestampStr
    }
}
