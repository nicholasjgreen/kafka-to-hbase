import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.properties.assertAll
import io.kotlintest.matchers.beInstanceOf
import io.kotlintest.specs.StringSpec
import com.beust.klaxon.JsonObject
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.*


class Conversion : StringSpec({
    configureLogging()
    
    val converter = Converter()

    "valid input converts to json" {
        val jsonString = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val json: JsonObject = converter.convertToJson(jsonString.toByteArray())

        json should beInstanceOf<JsonObject>()
        json.string("testOne") shouldBe "test1"
        json.int("testTwo") shouldBe 2
    }

    "valid nested input converts to json" {
        val jsonString = "{\"testOne\":{\"testTwo\":2}}"
        val json: JsonObject = converter.convertToJson(jsonString.toByteArray())
        val jsonTwo: JsonObject = json.obj("testOne") as JsonObject

        json should beInstanceOf<JsonObject>()
        jsonTwo.int("testTwo") shouldBe 2
    }

    "invalid nested input throws exception" {
        val jsonString = "{\"testOne\":"

        val exception = shouldThrow<IllegalArgumentException> {
            converter.convertToJson(jsonString.toByteArray())
        }
        exception.message shouldBe "Cannot parse invalid JSON"
    }

    "can generate consistent base64 encoded string" {
        val jsonStringWithFakeHash = "82&%\$dsdsd{\"testOne\":\"test1\", \"testTwo\":2}"
        val encodedStringOne = converter.encodeToBase64(jsonStringWithFakeHash)
        val encodedStringTwo = converter.encodeToBase64(jsonStringWithFakeHash)

        encodedStringOne shouldBe encodedStringTwo
    }

    "can encode and decode string with base64" {
        val jsonStringWithFakeHash = "82&%\$dsdsd{\"testOne\":\"test1\", \"testTwo\":2}"
        val encodedString = converter.encodeToBase64(jsonStringWithFakeHash)
        val decodedString = converter.decodeFromBase64(encodedString)

        decodedString shouldBe jsonStringWithFakeHash
    }

    "sorts json by key name" {
        val jsonStringUnsorted = "{\"testA\":\"test1\", \"testC\":2, \"testB\":true}"
        val jsonObjectUnsorted: JsonObject = converter.convertToJson(jsonStringUnsorted.toByteArray())
        val jsonStringSorted = "{\"testA\":\"test1\",\"testB\":true,\"testC\":2}"

        val sortedJson = converter.sortJsonByKey(jsonObjectUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    "sorts json by key name case sensitively" {
        val jsonStringUnsorted = "{\"testb\":true, \"testA\":\"test1\", \"testC\":2}"
        val jsonObjectUnsorted: JsonObject = converter.convertToJson(jsonStringUnsorted.toByteArray())
        val jsonStringSorted = "{\"testA\":\"test1\",\"testC\":2,\"testb\":true}"

        val sortedJson = converter.sortJsonByKey(jsonObjectUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    "checksums are different with different inputs" {
        val jsonStringOne = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val jsonStringTwo = "{\"testOne\":\"test2\", \"testTwo\":2}"
        val checksum = converter.generateFourByteChecksum(jsonStringOne)
        val checksumTwo = converter.generateFourByteChecksum(jsonStringTwo)

        checksum shouldNotBe checksumTwo
    }

    "can generate consistent checksums from json" {
        val jsonString = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val json: JsonObject = converter.convertToJson(jsonString.toByteArray())
        val checksumOne = converter.generateFourByteChecksum(json.toString())
        val checksumTwo = converter.generateFourByteChecksum(json.toString())

        checksumOne shouldBe checksumTwo
    }

    "generated checksums are four bytes" {
        assertAll { input: String ->
            val checksum = converter.generateFourByteChecksum(input)
            checksum.size shouldBe 4
        }
    }

    "valid timestamp format in the message gets parsed as long correctly" {
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

        val json: JsonObject = converter.convertToJson(jsonString.toByteArray())
        val timestamp = converter.getLastModifiedTimestamp(json)
        val timeStampAsLong = converter.getTimestampAsLong(timestamp)
        timestamp shouldBe "2018-12-14T15:01:02.000+0000"
        timeStampAsLong shouldBe 1544799662000
    }

    "Invalid timestamp format in the message throws Exception" {
        val jsonString = "{\n" +
            "        \"message\": {\n" +
            "            \"_lastModifiedDateTime\": \"2018-12-14\",\n" +
            "        }\n" +
            "    }"

        val json: JsonObject = converter.convertToJson(jsonString.toByteArray())
        val timestamp = converter.getLastModifiedTimestamp(json)
        timestamp shouldBe "2018-12-14"
        shouldThrow<ParseException> {
            converter.getTimestampAsLong(timestamp)
        }
    }

    "Invalid json with missing message attribute  throws Exception" {
        val jsonString = "{\n" +
            "        \"message1\": {\n" +
            "           \"_lastModifiedDateTime\": \"2018-12-14T15:01:02.000+0000\",\n" +
            "        }\n" +
            "    }"

        val json: JsonObject = converter.convertToJson(jsonString.toByteArray())
        shouldThrow<RuntimeException> {
            val lastModifiedTimestamp = converter.getLastModifiedTimestamp(json)
            converter.getTimestampAsLong(lastModifiedTimestamp)
        }
    }

    "Invalid json with missing _lastModifiedDateTime attribute  throws Exception" {
        val jsonString = "{\n" +
            "        \"message\": {\n" +
            "           \"_lastModifiedDateTime1\": \"2018-12-14T15:01:02.000+0000\",\n" +
            "        }\n" +
            "    }"

        val json: JsonObject = converter.convertToJson(jsonString.toByteArray())
        shouldThrow<RuntimeException> {
            val lastModifiedTimestamp = converter.getLastModifiedTimestamp(json)
            converter.getTimestampAsLong(lastModifiedTimestamp)
        }
    }

    "Invalid json with  _lastModifiedDateTime attribute value as empty  throws Exception" {
        val jsonString = "{\n" +
            "        \"message\": {\n" +
            "           \"_lastModifiedDateTime\": \"\",\n" +
            "        }\n" +
            "    }"

        val json: JsonObject = converter.convertToJson(jsonString.toByteArray())
        shouldThrow<RuntimeException> {
            val lastModifiedTimestamp = converter.getLastModifiedTimestamp(json)
            converter.getTimestampAsLong(lastModifiedTimestamp)
        }
    }

    "Invalid json with  _lastModifiedDateTime attribute value as blank  throws Exception" {
        val jsonString = "{\n" +
            "        \"message\": {\n" +
            "           \"_lastModifiedDateTime\": \"   \",\n" +
            "        }\n" +
            "    }"

        val json: JsonObject = converter.convertToJson(jsonString.toByteArray())
        shouldThrow<RuntimeException> {
            val lastModifiedTimestamp = converter.getLastModifiedTimestamp(json)
            converter.getTimestampAsLong(lastModifiedTimestamp)
        }
    }

    "Invalid  with  _lastModifiedDateTime attribute value as blank  throws Exception" {
        val tz = TimeZone.getTimeZone("UTC")
        val df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ") // Quoted "Z" to indicate UTC, no timezone offset
        df.timeZone = tz
        println(df.format(Date()))
    }
})
