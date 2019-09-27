import com.beust.klaxon.JsonObject
import java.util.logging.Logger

open class MessageParser {

    private val converter = Converter()
    private val log: Logger = Logger.getLogger("messageParser")

    open fun generateKeyFromRecordBody(body: JsonObject?): ByteArray {
        val id: JsonObject? = body?.let { getId(it) }
        return if (id == null) ByteArray(0) else generateKey(id)
    }

    fun getId(json: JsonObject): JsonObject? {
        try {
            val message: JsonObject? = json.obj("message")
            return if (message == null) null else message.obj("_id")
        } catch (e: ClassCastException) {
            log.warning("Record body does not contain valid json object at message._id")
            return null
        }
    }

    fun generateKey(json: JsonObject): ByteArray {
        val jsonOrdered = converter.sortJsonByKey(json)
        val checksumBytes: ByteArray = converter.generateFourByteChecksum(jsonOrdered)

        return checksumBytes.plus(jsonOrdered.toByteArray())
    }
}
