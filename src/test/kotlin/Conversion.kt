import com.beust.klaxon.JsonObject
import io.kotlintest.matchers.beInstanceOf
import io.kotlintest.properties.assertAll
import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.*


class Conversion : StringSpec({
    configureLogging()

    "valid input converts to json" {
        val json_string = "{\"testOne\":\"test1\", \"testTwo\":2}"

        val json: JsonObject = convertToJson(json_string.toByteArray())

        json should beInstanceOf<JsonObject>()
        json.string("testOne") shouldBe "test1"
        json.int("testTwo") shouldBe 2
    }

    "valid nested input converts to json" {
        val json_string = "{\"testOne\":{\"testTwo\":2}}"

        val json: JsonObject = convertToJson(json_string.toByteArray())
        val json_two: JsonObject = json.obj("testOne") as JsonObject

        json should beInstanceOf<JsonObject>()
        json_two.int("testTwo") shouldBe 2
    }

    "invalid nested input throws exception" {
        val json_string = "{\"testOne\":"

        val exception = shouldThrow<IllegalArgumentException> {
            convertToJson(json_string.toByteArray())
        }

        exception.message shouldBe "Cannot parse invalid JSON"
    }

    "can generate consistent base64 encoded string" {
        val json_string_with_fake_hash = "82&%\$dsdsd{\"testOne\":\"test1\", \"testTwo\":2}"

        val encodedStringOne = encodeToBase64(json_string_with_fake_hash)
        val encodedStringTwo = encodeToBase64(json_string_with_fake_hash)

        encodedStringOne shouldBe encodedStringTwo
    }

    "can encode and decode string with base64" {
        val json_string_with_fake_hash = "82&%\$dsdsd{\"testOne\":\"test1\", \"testTwo\":2}"

        val encodedString = encodeToBase64(json_string_with_fake_hash)
        val decodedString = decodeFromBase64(encodedString)

        decodedString shouldBe json_string_with_fake_hash
    }

    "sorts json by key name" {
        val jsonStringUnsorted = "{\"testA\":\"test1\", \"testC\":2, \"testb\":true}"
        val jsonObjectUnsorted: JsonObject = convertToJson(jsonStringUnsorted.toByteArray())
        val jsonStringSorted = "testA=test1,testb=true,testC=2"

        val sortedJson = sortJsonByKey(jsonObjectUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    "checksums are different with different inputs" {
        val jsonStringOne = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val jsonStringTwo = "{\"testOne\":\"test2\", \"testTwo\":2}"
        val checksum = generateFourByteChecksum(jsonStringOne)
        val checksumTwo = generateFourByteChecksum(jsonStringTwo)

        checksum shouldNotBe checksumTwo
    }

    "can generate consistent checksums from json" {
        val json_string = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val json: JsonObject = convertToJson(json_string.toByteArray())
        val checksumOne = generateFourByteChecksum(json.toString())
        val checksumTwo = generateFourByteChecksum(json.toString())

        checksumOne shouldBe checksumTwo
    }

    "generated checksums are four bytes" {
        assertAll({ input: String ->
            val checksum = generateFourByteChecksum(input)
            checksum.size shouldBe 4
        })
    }

    "valid timestamp format in the message gets parsed as long correctly" {
        val json_string = "{\n" +
            "        \"traceId\": \"00001111-abcd-4567-1234-1234567890ab\",\n" +
            "        \"unitOfWorkId\": \"00002222-abcd-4567-1234-1234567890ab\",\n" +
            "        \"@type\": \"V4\",\n" +
            "        \"version\": \"core-X.release_XXX.XX\",\n" +
            "        \"timestamp\": \"2018-12-14T15:01:02.000+0000\",\n" +
            "        \"message\": {\n" +
            "            \"@type\": \"MONGO_UPDATE\",\n" +
            "            \"collection\": \"exampleCollectionName\",\n" +
            "            \"db\": \"exampleDbName\",\n" +
            "            \"_id\": {\n" +
            "                \"exampleId\": \"aaaa1111-abcd-4567-1234-1234567890ab\"\n" +
            "            },\n" +
            "            \"_lastModifiedDateTime\": \"2018-12-14T15:01:02.000+0000\",\n" +
            "            \"encryption\": {\n" +
            "                \"encryptionKeyId\": \"55556666-abcd-89ab-1234-1234567890ab\",\n" +
            "                \"encryptedEncryptionKey\": \"bHJjhg2Jb0uyidkl867gtFkjl4fgh9Ab\",\n" +
            "                \"initialisationVector\": \"kjGyvY67jhJHVdo2\",\n" +
            "                \"keyEncryptionKeyId\": \"example-key_2019-12-14_01\"\n" +
            "            },\n" +
            "            \"dbObject\": \"bubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9A\"\n" +
            "        }\n" +
            "    }"

        val json: JsonObject = convertToJson(json_string.toByteArray())
        val timestamp = getLastModifiedTimestamp(json)
        val timeStampAsLong = getTimestampAsLong(timestamp)
        timestamp shouldBe "2018-12-14T15:01:02.000+0000"
        timeStampAsLong shouldBe 1544799662000
    }

    "Invalid timestamp format in the message throws Exception" {
        val json_string = "{\n" +
            "        \"message\": {\n" +
            "            \"_lastModifiedDateTime\": \"2018-12-14\",\n" +
            "        }\n" +
            "    }"

        val json: JsonObject = convertToJson(json_string.toByteArray())
        val timestamp = getLastModifiedTimestamp(json)
        timestamp shouldBe "2018-12-14"
        shouldThrow<ParseException> {
            getTimestampAsLong(timestamp)
        }
    }

    "Invalid json with missing message attribute  throws Exception" {
        val json_string = "{\n" +
            "        \"message1\": {\n" +
            "           \"_lastModifiedDateTime\": \"2018-12-14T15:01:02.000+0000\",\n" +
            "        }\n" +
            "    }"

        val json: JsonObject = convertToJson(json_string.toByteArray())
        shouldThrow<RuntimeException> {
            val lastModifiedTimestamp = getLastModifiedTimestamp(json)
            getTimestampAsLong(lastModifiedTimestamp)
        }
    }

    "Invalid json with missing _lastModifiedDateTime attribute  throws Exception" {
        val json_string = "{\n" +
            "        \"message\": {\n" +
            "           \"_lastModifiedDateTime1\": \"2018-12-14T15:01:02.000+0000\",\n" +
            "        }\n" +
            "    }"

        val json: JsonObject = convertToJson(json_string.toByteArray())
        shouldThrow<RuntimeException> {
            val lastModifiedTimestamp = getLastModifiedTimestamp(json)
            getTimestampAsLong(lastModifiedTimestamp)
        }
    }

    "Invalid json with  _lastModifiedDateTime attribute value as empty  throws Exception" {
        val json_string = "{\n" +
            "        \"message\": {\n" +
            "           \"_lastModifiedDateTime\": \"\",\n" +
            "        }\n" +
            "    }"

        val json: JsonObject = convertToJson(json_string.toByteArray())
        shouldThrow<RuntimeException> {
            val lastModifiedTimestamp = getLastModifiedTimestamp(json)
            getTimestampAsLong(lastModifiedTimestamp)
        }
    }

    "Invalid json with  _lastModifiedDateTime attribute value as blank  throws Exception" {
        val json_string = "{\n" +
            "        \"message\": {\n" +
            "           \"_lastModifiedDateTime\": \"   \",\n" +
            "        }\n" +
            "    }"

        val json: JsonObject = convertToJson(json_string.toByteArray())
        shouldThrow<RuntimeException> {
            val lastModifiedTimestamp = getLastModifiedTimestamp(json)
            getTimestampAsLong(lastModifiedTimestamp)
        }
    }

    "Invalid  with  _lastModifiedDateTime attribute value as blank  throws Exception" {
        val tz = TimeZone.getTimeZone("UTC")
        val df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ") // Quoted "Z" to indicate UTC, no timezone offset
        df.timeZone = tz
        println(df.format(Date()))
    }
})