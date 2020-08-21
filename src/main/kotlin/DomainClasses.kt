import org.apache.kafka.clients.consumer.ConsumerRecord

data class HbasePayload(
        val key: ByteArray,
        val body: ByteArray,
        val version: Long,
        val record: ConsumerRecord<ByteArray, ByteArray>) {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as HbasePayload

        if (!key.contentEquals(other.key)) return false
        if (!body.contentEquals(other.body)) return false
        if (version != other.version) return false

        return true
    }

    override fun hashCode(): Int {
        var result = key.contentHashCode()
        result = 31 * result + body.contentHashCode()
        result = 31 * result + version.hashCode()
        return result
    }
}
