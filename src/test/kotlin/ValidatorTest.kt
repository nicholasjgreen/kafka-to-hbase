import io.kotlintest.matchers.string.shouldStartWith
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec

class ValidatorTest: StringSpec({

    configureLogging()

    "Valid message passes validation." {
        Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "collection" : "addresses",
            |       "db": "core",
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
        """.trimMargin())
    }

    "Additional properties allowed." {
        Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "collection" : "addresses",
            |       "db": "core",
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "==",
            |           "additional": [0, 1, 2, 3, 4]
            |       },
            |       "additional": [0, 1, 2, 3, 4]
            |   },
            |   "additional": [0, 1, 2, 3, 4]
            |}
        """.trimMargin())
    }

    "Missing '#/message' causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "msg": [0, 1, 2]
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe   "Message failed schema validation: '#: required key [message] not found'."
    }

    "Incorrect '#/message' type causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": 123
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe   "Message failed schema validation: '#/message: expected type: JSONObject, found: Integer'."
    }

    "Missing '#/message/@type' field causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
                |       "_id": {
            |           "declarationId": 1
            |       },
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "collection" : "addresses",
            |       "db": "core",
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe   "Message failed schema validation: '#/message: required key [@type] not found'."
    }

    "Incorrect '#/message/@type' type causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "@type": 1,
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "collection" : "addresses",
            |       "db": "core",
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message/@type: expected type: String, found: Integer'."
    }

    "Missing '#/message/_id' field causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "db": "abcd",
            |       "collection" : "addresses",
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe   "Message failed schema validation: '#/message: required key [_id] not found'."
    }

    "Incorrect '#/message/_id' type causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": [1, 2, 3, 4, 5, 6, 7 ,8 , 9],
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "db": "abcd",
            |       "collection" : "addresses",
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe   "Message failed schema validation: '#/message/_id: expected type: JSONObject, found: JSONArray'."
    }

    "Empty '#/message/_id' type causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {},
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "db": "abcd",
            |       "collection" : "addresses",
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message/_id: minimum size: [1], found: [0]'."
    }

    "Missing '#/message/_lastModifiedDateTime' causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": { part: 1},
            |       "db": "abcd",
            |       "collection" : "addresses",
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message: required key [_lastModifiedDateTime] not found'."
    }

    "Incorrect '#/message/_lastModifiedDateTime' type causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": { part: 1},
            |       "_lastModifiedDateTime": 12,
            |       "db": "abcd",
            |       "collection" : "addresses",
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message/_lastModifiedDateTime: expected type: String, found: Integer'."
    }

    "Incorrect '#/message/_lastModifiedDateTime' format causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": { part: 1},
            |       "_lastModifiedDateTime": "2019-07-04",
            |       "db": "abcd",
            |       "collection" : "addresses",
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message/_lastModifiedDateTime: string [2019-07-04] does not match pattern ^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}\\+\\d{4}\$'."
    }

    "Missing '#/message/db' field causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "collection" : "addresses",
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe   "Message failed schema validation: '#/message: required key [db] not found'."
    }

    "Incorrect '#/message/db' type  causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "db": [0, 1, 2],
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "collection" : "addresses",
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe  "Message failed schema validation: '#/message/db: expected type: String, found: JSONArray'."
    }

    "Empty '#/message/db' causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "db": "",
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "collection" : "addresses",
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message/db: expected minLength: 1, actual: 0'."
    }

    "Missing '#/message/collection' field causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "db" : "addresses",
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe   "Message failed schema validation: '#/message: required key [collection] not found'."
    }

    "Incorrect '#/message/collection' type  causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "db" : "addresses",
            |       "collection": 5,
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe  "Message failed schema validation: '#/message/collection: expected type: String, found: Integer'."
    }

    "Empty '#/message/collection' causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "db": "address",
            |       "collection": "",
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "dbObject": "asd",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message/collection: expected minLength: 1, actual: 0'."
    }


    "Missing '#/message/dbObject' field causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "db" : "addresses",
            |       "collection": "core",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message: required key [dbObject] not found'."
    }

    "Incorrect '#/message/dbObject' type  causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "db" : "addresses",
            |       "collection": "collection",
            |       "dbObject": { "key": "value" },
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe  "Message failed schema validation: '#/message/dbObject: expected type: String, found: JSONObject'."
    }

    "Empty '#/message/dbObject' causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "db": "address",
            |       "collection": "collection",
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "dbObject": "",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message/dbObject: expected minLength: 1, actual: 0'."
    }

    "Missing '#/message/encryption' causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "db": "address",
            |       "collection": "collection",
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "dbObject": "123"
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message: required key [encryption] not found'."
    }

    "Incorrect '#/message/encryption' type causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "db": "address",
            |       "collection": "collection",
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "dbObject": "123",
            |       "encryption": "hello"
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message/encryption: expected type: JSONObject, found: String'."
    }

    "Missing keyEncryptionKeyId from '#/message/encryption' type causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "db": "address",
            |       "collection": "collection",
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "dbObject": "123",
            |       "encryption": {
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message/encryption: required key [keyEncryptionKeyId] not found'."
    }

    "Missing initialisationVector from '#/message/encryption' type causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "db": "address",
            |       "collection": "collection",
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "dbObject": "123",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:1,2",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message/encryption: required key [initialisationVector] not found'."
    }

    "Missing encryptedEncryptionKey from '#/message/encryption' type causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "db": "address",
            |       "collection": "collection",
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "dbObject": "123",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv"
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message/encryption: required key [encryptedEncryptionKey] not found'."
    }

    "Empty keyEncryptionKeyId from '#/message/encryption' type causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "db": "address",
            |       "collection": "collection",
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "dbObject": "123",
            |       "encryption": {
            |           "keyEncryptionKeyId": "",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message/encryption/keyEncryptionKeyId: string [] does not match pattern ^cloudhsm:\\d+,\\d+\$'."
    }

    "Empty initialisationVector from '#/message/encryption' type causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "db": "address",
            |       "collection": "collection",
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "dbObject": "123",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:1,2",
            |           "initialisationVector": "",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message/encryption/initialisationVector: expected minLength: 1, actual: 0'."
    }

    "Empty encryptedEncryptionKey from '#/message/encryption' type causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "db": "address",
            |       "collection": "collection",
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "dbObject": "123",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": ""
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message/encryption/encryptedEncryptionKey: expected minLength: 1, actual: 0'."
    }

    "Incorrect '#/message/encryption/keyEncryptionKeyId' type causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "db": "address",
            |       "collection": "collection",
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "dbObject": "123",
            |       "encryption": {
            |           "keyEncryptionKeyId": 0,
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message/encryption/keyEncryptionKeyId: expected type: String, found: Integer'."
    }

    "Incorrect initialisationVector '#/message/encryption/initialisationVector' type causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "db": "address",
            |       "collection": "collection",
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "dbObject": "123",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:1,2",
            |           "initialisationVector": {},
            |           "encryptedEncryptionKey": "=="
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message/encryption/initialisationVector: expected type: String, found: JSONObject'."
    }

    "Incorrect '#/message/encryption/encryptedEncryptionKey' type causes validation failure." {
        val exception = shouldThrow<InvalidMessageException> {
            Validator().validate("""
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "db": "address",
            |       "collection": "collection",
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
            |       "dbObject": "123",
            |       "encryption": {
            |           "keyEncryptionKeyId": "cloudhsm:7,14",
            |           "initialisationVector": "iv",
            |           "encryptedEncryptionKey": [0, 1, 2]
            |       }
            |   }
            |}
            """.trimMargin()
            )
        }
        exception.message shouldBe "Message failed schema validation: '#/message/encryption/encryptedEncryptionKey: expected type: String, found: JSONArray'."
    }
})