import java.util.logging.Logger
import java.util.Base64
import java.util.zip.CRC32
import java.nio.ByteBuffer
import com.beust.klaxon.Parser
import com.beust.klaxon.JsonObject
import com.beust.klaxon.KlaxonException
import com.beust.klaxon.lookup
import java.lang.RuntimeException
import java.text.SimpleDateFormat

class Converter() {
    private val log: Logger = Logger.getLogger("Converter")

    fun convertToJson(body: ByteArray): JsonObject {

        try {
            val parser: Parser = Parser.default()
            val stringBuilder: StringBuilder = StringBuilder(String(body))
            val json: JsonObject = parser.parse(stringBuilder) as JsonObject
            return json
        } catch (e: KlaxonException) {
            log.warning(
                "Error while parsing message body of '%s' in to json: %s".format(
                    String(body),
                    e.toString()
                )
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

    fun getTimestampAsLong(timeStampAsStr: String?, timeStampPattern: String = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ"): Long {
        val df = SimpleDateFormat(timeStampPattern);
        return df.parse(timeStampAsStr).time
    }

    fun getLastModifiedTimestamp(json: JsonObject): String? {
        val lastModifiedTimestampStr = json.lookup<String?>("message._lastModifiedDateTime").get(0)
        if (lastModifiedTimestampStr.isNullOrBlank()) throw RuntimeException("Last modified date time is null or blank")
        return lastModifiedTimestampStr
    }
}
