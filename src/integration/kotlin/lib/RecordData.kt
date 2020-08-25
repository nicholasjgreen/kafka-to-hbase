package lib

import java.text.SimpleDateFormat
import java.time.Instant
import java.util.*

fun getId() = """{ "exampleId": "aaaa1111-abcd-4567-1234-1234567890ab"}"""

fun getEqualityId() = """{ "messageId": "aaaa1111-abcd-4567-4321-1234567890ab"}"""


fun wellFormedValidPayload(collectionName: String = "exampleCollectionName",
                           dbName: String = "exampleDbName") = """{
        "traceId": "00001111-abcd-4567-1234-1234567890ab",
        "unitOfWorkId": "00002222-abcd-4567-1234-1234567890ab",
        "@type": "V4",
        "version": "core-X.release_XXX.XX",
        "timestamp": "2018-12-14T15:01:02.000+0000",
        "message": {
            "@type": "MONGO_UPDATE",
            "collection": "$collectionName",
            "db": "$dbName",
            "_id": ${getId()},
            "_lastModifiedDateTime": "${getISO8601Timestamp()}",
            "encryption": {
                "encryptionKeyId": "cloudhsm:1,2",
                "encryptedEncryptionKey": "bHJjhg2Jb0uyidkl867gtFkjl4fgh9Ab",
                "initialisationVector": "kjGyvY67jhJHVdo2",
                "keyEncryptionKeyId": "cloudhsm:1,2"
            },
            "dbObject": "bubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9A",
            "timestamp_created_from": "_lastModifiedDateTime"
        }
    }""".toByteArray()

fun wellFormedValidPayloadEquality() = """{
        "traceId": "00001111-abcd-4567-4321-1234567890ab",
        "unitOfWorkId": "00002222-abcd-7654-1234-1234567890ab",
		"@type": "V4",
		"version": "core-X.release_XXX.X",
		"timestamp": "2020-05-21T17:18:15.706+0000",
		"message": {
			"@type": "EQUALITY_QUESTIONS_ANSWERED",
            "_id": ${getEqualityId()},
			"_lastModifiedDateTime": "${getISO8601Timestamp()}",
			"encryption": {
				"keyEncryptionKeyId": "cloudhsm:1,2",
				"encryptedEncryptionKey": "bHJjhg2Jb0uyidkl867gtFkjl4fgh9Ab",
				"initialisationVector": "kjGyvY67jhJHVdo2"
			},
			"dbObject": "xxxxxx",
            "timestamp_created_from": "_lastModifiedDateTime"
		}
	}""".toByteArray()

fun getISO8601Timestamp(): String {
    val tz = TimeZone.getTimeZone("UTC")
    val df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ")
    df.timeZone = tz
    return df.format(Date())
}

fun uniqueTopicName() = "db.database.collection_${Instant.now().toEpochMilli()}"

fun uniqueEqualityTopicName() = "data.equality_${Instant.now().toEpochMilli()}"

fun sampleQualifiedTableName(namespace: String, tableName: String) =
    "$namespace:$tableName".replace("-", "_").replace(".", "_")
