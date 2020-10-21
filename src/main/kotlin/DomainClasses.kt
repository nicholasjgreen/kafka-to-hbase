import org.apache.kafka.clients.consumer.ConsumerRecord

data class HbasePayload(val key: ByteArray, val body: ByteArray, val id: String, val version: Long, val versionCreatedFrom: String, val versionRaw: String, val record: ConsumerRecord<ByteArray, ByteArray>) {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as HbasePayload

        if (!key.contentEquals(other.key)) return false
        if (!id.contentEquals(other.id)) return false
        if (!body.contentEquals(other.body)) return false
        if (version != other.version) return false
        if (versionRaw != other.versionRaw) return false
        if (versionCreatedFrom != other.versionCreatedFrom) return false

        return true
    }

    override fun hashCode(): Int {
        var result = key.contentHashCode()
        result = 31 * result + id.hashCode()
        result = 31 * result + body.contentHashCode()
        result = 31 * result + version.hashCode()
        result = 31 * result + versionRaw.hashCode()
        result = 31 * result + versionCreatedFrom.hashCode()
        return result
    }

    override fun toString(): String {
        return """{ 
            |key: '${String(key)}', 
            |id: '${id}', 
            |body: '${String(body)}',
            |record: {
            |   offset: ${record.offset()},
            |   partition: ${record.partition()},
            |   topic: '${record.topic()}'
            |}
|       }""".trimMargin().replace(Regex("""\s+"""), " ")
    }
}

data class ManifestRecord(val id: String, val timestamp: Long, val db: String, val collection: String,
                          val source: String, val externalOuterSource: String, val externalInnerSource: String,
                          val originalId: String)
