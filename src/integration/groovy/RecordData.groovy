import java.time.LocalDateTime
import java.time.ZoneOffset

class RecordData {
    static def uniqueBytes() {
        return UUID.randomUUID().toString().getBytes()
    }

    static def timestamp() {
        return LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli()
    }
}

