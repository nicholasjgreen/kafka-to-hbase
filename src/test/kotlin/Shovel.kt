import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.StringSpec
import com.beust.klaxon.JsonObject

class Shovel : StringSpec({
    configureLogging()

    "generated key is consistent for identical inputs" {
        val json: JsonObject = convertToJson("{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray())

        val keyOne: ByteArray = generateKey(json)
        val keyTwo: ByteArray = generateKey(json)

        keyOne shouldBe keyTwo
    }

    "generated keys are different for different inputs" {
        val jsonOne: JsonObject = convertToJson("{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray())
        val jsonTwo: JsonObject = convertToJson("{\"testOne\":\"test1\", \"testTwo\":3}".toByteArray())

        val keyOne: ByteArray = generateKey(jsonOne)
        val keyTwo: ByteArray = generateKey(jsonTwo)

        keyOne shouldNotBe keyTwo
    }

    "generated key is consistent for identical inputs regardless of order" {
        val jsonOne: JsonObject = convertToJson("{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray())
        val jsonTwo: JsonObject = convertToJson("{\"testTwo\":2, \"testOne\":\"test1\"}".toByteArray())

        val keyOne: ByteArray = generateKey(jsonOne)
        val keyTwo: ByteArray = generateKey(jsonTwo)

        keyOne shouldBe keyTwo
    }

    "generated key is consistent for identical inputs regardless of whitespace" {
        val jsonOne: JsonObject = convertToJson("{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray())
        val jsonTwo: JsonObject = convertToJson("{    \"testOne\":              \"test1\",            \"testTwo\":  2}".toByteArray())

        val keyOne: ByteArray = generateKey(jsonOne)
        val keyTwo: ByteArray = generateKey(jsonTwo)

        keyOne shouldBe keyTwo
    }

    "generated key is consistent for identical inputs regardless of order and whitespace" {
        val jsonOne: JsonObject = convertToJson("{\"testOne\":\"test1\", \"testTwo\":2}".toByteArray())
        val jsonTwo: JsonObject = convertToJson("{    \"testTwo\":              2,            \"testOne\":  \"test1\"}".toByteArray())

        val keyOne: ByteArray = generateKey(jsonOne)
        val keyTwo: ByteArray = generateKey(jsonTwo)

        keyOne shouldBe keyTwo
    }
})