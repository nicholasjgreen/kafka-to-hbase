package lib

import java.text.SimpleDateFormat
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*

const val idString = "{\n" +
        "                \"exampleId\": \"aaaa1111-abcd-4567-1234-1234567890ab\"\n" +
        "            }"

fun getId(): String {
    return idString
}

fun uniqueBytes(): ByteArray {
    val jsonString = "{\n" +
            "        \"traceId\": \"00001111-abcd-4567-1234-1234567890ab\",\n" +
            "        \"unitOfWorkId\": \"00002222-abcd-4567-1234-1234567890ab\",\n" +
            "        \"@type\": \"V4\",\n" +
            "        \"version\": \"core-X.release_XXX.XX\",\n" +
            "        \"timestamp\": \"2018-12-14T15:01:02.000+0000\",\n" +
            "        \"message\": {\n" +
            "            \"@type\": \"MONGO_UPDATE\",\n" +
            "            \"collection\": \"exampleCollectionName\",\n" +
            "            \"db\": \"exampleDbName\",\n" +
            "            \"_id\": ${getId()},\n" +
            "            \"_lastModifiedDateTime\": \"${getISO8601Timestamp()} \",\n" +
            "            \"encryption\": {\n" +
            "                \"encryptionKeyId\": \"55556666-abcd-89ab-1234-1234567890ab\",\n" +
            "                \"encryptedEncryptionKey\": \"bHJjhg2Jb0uyidkl867gtFkjl4fgh9Ab\",\n" +
            "                \"initialisationVector\": \"kjGyvY67jhJHVdo2\",\n" +
            "                \"keyEncryptionKeyId\": \"example-key_2019-12-14_01\"\n" +
            "            },\n" +
            "            \"dbObject\": \"bubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9A\"\n" +
            "        }\n" +
            "    }"

    return jsonString.toByteArray()
}

fun uniqueBytesNoId(): ByteArray {
    val jsonString = "{\n" +
            "        \"traceId\": \"00001111-abcd-4567-1234-1234567890ab\",\n" +
            "        \"unitOfWorkId\": \"00002222-abcd-4567-1234-1234567890ab\",\n" +
            "        \"@type\": \"V4\",\n" +
            "        \"version\": \"core-X.release_XXX.XX\",\n" +
            "        \"timestamp\": \"2018-12-14T15:01:02.000+0000\",\n" +
            "        \"message\": {\n" +
            "            \"@type\": \"MONGO_UPDATE\",\n" +
            "            \"collection\": \"exampleCollectionName\",\n" +
            "            \"db\": \"exampleDbName\",\n" +
            "            \"id\": ${getId()},\n" +
            "            \"_lastModifiedDateTime\": \"${getISO8601Timestamp()} \",\n" +
            "            \"encryption\": {\n" +
            "                \"encryptionKeyId\": \"55556666-abcd-89ab-1234-1234567890ab\",\n" +
            "                \"encryptedEncryptionKey\": \"bHJjhg2Jb0uyidkl867gtFkjl4fgh9Ab\",\n" +
            "                \"initialisationVector\": \"kjGyvY67jhJHVdo2\",\n" +
            "                \"keyEncryptionKeyId\": \"example-key_2019-12-14_01\"\n" +
            "            },\n" +
            "            \"dbObject\": \"bubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9A\"\n" +
            "        }\n" +
            "    }"

    return jsonString.toByteArray()
}

fun getISO8601Timestamp(): String {
    val tz = TimeZone.getTimeZone("UTC")
    val df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ")
    df.timeZone = tz
    return df.format(Date())
}

fun timestamp(): Long {
    return LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli()
}

fun uniqueTopicName(): ByteArray {
    val time = Instant.now().toEpochMilli()
    return "test-topic-$time".toByteArray()
}