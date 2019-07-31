package lib

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*

fun uniqueBytes(): ByteArray {
    return UUID.randomUUID().toString().toByteArray()
}

fun timestamp(): Long {
    return LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli()
}

fun uniqueTopicName(): ByteArray {
    return "test-topic-%s".format(Instant.now().toEpochMilli()).toByteArray()
}