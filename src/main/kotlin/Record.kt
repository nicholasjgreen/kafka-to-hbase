data class Record(
    val topic: ByteArray,
    val key: ByteArray,
    val value: ByteArray,
    val timestamp: Long,
    val partition: Int,
    val offset: Long
) {
    override fun equals(other: Any?) =
        this === other ||
                other is Record &&
                javaClass == other.javaClass &&
                topic contentEquals other.topic &&
                key contentEquals other.key &&
                value contentEquals other.value &&
                timestamp == other.timestamp &&
                partition == other.partition &&
                offset == other.offset

    override fun hashCode() =
        arrayOf(
            topic.contentHashCode(),
            key.contentHashCode(),
            value.contentHashCode(),
            timestamp.hashCode(),
            partition,
            offset.hashCode()
        ).reduce { hashCode, value -> 31 * hashCode + value }
}