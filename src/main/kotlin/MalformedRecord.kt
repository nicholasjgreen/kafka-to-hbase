import java.io.Serializable

data class MalformedRecord(val key: String, val body: String, val reason: String) : Serializable

