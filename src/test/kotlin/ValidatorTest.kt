import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class ValidatorTest : StringSpec() {

    init {
        "Default schema: Valid message passes validation." {
            TestUtils.defaultMessageValidator()

            Validator().validate(
                """
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
        """.trimMargin()
            )
        }

        "Default schema: Valid message alternate date format passes validation." {
            TestUtils.defaultMessageValidator()

            Validator().validate(
                """
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "_lastModifiedDateTime": "2019-07-04T07:27:35.104",
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

        "Default schema: Valid message alternate date format number two passes validation." {
            TestUtils.defaultMessageValidator()

            Validator().validate(
                """
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "_lastModifiedDateTime": "2017-06-19T23:00:10.875Z",
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

        "Default schema: Additional properties allowed." {
            TestUtils.defaultMessageValidator()

            Validator().validate(
                """
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
        """.trimMargin()
            )
        }

        "Missing '#/message' causes validation failure." {
            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
            |{
            |   "msg": [0, 1, 2]
            |}
            """.trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#: required key [message] not found'."
        }

        "Incorrect '#/message' type causes validation failure." {
            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
            |{
            |   "message": 123
            |}
            """.trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message: expected type: JSONObject, found: Integer'."
        }

        "Default schema: Missing '#/message/@type' field causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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
            exception.message shouldBe "Message failed schema validation: '#/message: required key [@type] not found'."
        }

        "Default schema: Incorrect '#/message/@type' type causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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

        "Default schema: String '#/message/_id' field does not cause validation failure." {
            TestUtils.defaultMessageValidator()

            Validator().validate(
                """
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": "abcdefg",
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

        "Default schema: Integer '#/message/_id' field does not cause validation failure." {
            TestUtils.defaultMessageValidator()

            Validator().validate(
                """
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": 12345,
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


        "Default schema: Empty string '#/message/_id' field causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": "",
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

            exception.message shouldBe "Message failed schema validation: '#/message/_id: #: no subschema matched out of the total 3 subschemas'."
        }

        "Default schema: Incorrect '#/message/_id' type causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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
            exception.message shouldBe "Message failed schema validation: '#/message/_id: #: no subschema matched out of the total 3 subschemas'."
        }

        "Default schema: Empty '#/message/_id' type causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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
            exception.message shouldBe "Message failed schema validation: '#/message/_id: #: no subschema matched out of the total 3 subschemas'."
        }

        "Default schema: Missing '#/message/_lastModifiedDateTime' does not cause validation failure." {
            TestUtils.defaultMessageValidator()

            Validator().validate(
                """
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

        "Default schema: Null '#/message/_lastModifiedDateTime' does not cause validation failure." {
            TestUtils.defaultMessageValidator()

            Validator().validate(
                """
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "_lastModifiedDateTime": null,
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

        "Default schema: Empty '#/message/_lastModifiedDateTime' does not cause validation failure." {
            TestUtils.defaultMessageValidator()

            Validator().validate(
                """
            |{
            |   "message": {
            |       "@type": "hello",
            |       "_id": {
            |           "declarationId": 1
            |       },
            |       "_lastModifiedDateTime": "",
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


        "Default schema: Incorrect '#/message/_lastModifiedDateTime' type causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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
            exception.message shouldBe "Message failed schema validation: '#/message/_lastModifiedDateTime: #: no subschema matched out of the total 2 subschemas'."
        }

        "Default schema: Incorrect '#/message/_lastModifiedDateTime' format causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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
            exception.message shouldBe "Message failed schema validation: '#/message/_lastModifiedDateTime: #: no subschema matched out of the total 2 subschemas'."
        }

        "Default schema: Missing '#/message/db' field causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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
            exception.message shouldBe "Message failed schema validation: '#/message: required key [db] not found'."
        }

        "Default schema: Incorrect '#/message/db' type  causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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
            exception.message shouldBe "Message failed schema validation: '#/message/db: expected type: String, found: JSONArray'."
        }

        "Default schema: Empty '#/message/db' causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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

        "Default schema: Missing '#/message/collection' field causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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
            exception.message shouldBe "Message failed schema validation: '#/message: required key [collection] not found'."
        }

        "Default schema: Incorrect '#/message/collection' type  causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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
            exception.message shouldBe "Message failed schema validation: '#/message/collection: expected type: String, found: Integer'."
        }

        "Default schema: Empty '#/message/collection' causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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


        "Default schema: Missing '#/message/dbObject' field causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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

        "Default schema: Incorrect '#/message/dbObject' type  causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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
            exception.message shouldBe "Message failed schema validation: '#/message/dbObject: expected type: String, found: JSONObject'."
        }

        "Default schema: Empty '#/message/dbObject' causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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

        "Default schema: Missing '#/message/encryption' causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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

        "Default schema: Incorrect '#/message/encryption' type causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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

        "Default schema: Missing keyEncryptionKeyId from '#/message/encryption' type causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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

        "Default schema: Missing initialisationVector from '#/message/encryption' type causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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

        "Default schema: Missing encryptedEncryptionKey from '#/message/encryption' type causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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

        "Default schema: Empty keyEncryptionKeyId from '#/message/encryption' type causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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

        "Default schema: Empty initialisationVector from '#/message/encryption' type causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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

        "Default schema: Empty encryptedEncryptionKey from '#/message/encryption' type causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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

        "Default schema: Incorrect '#/message/encryption/keyEncryptionKeyId' type causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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

        "Default schema: Incorrect initialisationVector '#/message/encryption/initialisationVector' type causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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

        "Default schema: Incorrect '#/message/encryption/encryptedEncryptionKey' type causes validation failure." {
            TestUtils.defaultMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
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

        "Equality schema: Valid message passes validation." {
            TestUtils.equalityMessageValidator()

            Validator().validate(
                """
            |{
            |   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
            |   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
            |   "@type" : "V4",
            |   "message" : {
            |       "dbObject" : "xxxxxx",
            |       "encryption" : {
            |           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
            |           "encryptedEncryptionKey" : "xxxxxx",
            |           "initialisationVector" : "xxxxxxxx=="
            |       },
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.693+0000",
            |       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_id" : {
            |           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
            |       }
            |   },
            |   "version" : "core-4.release_147.3",
            |   "timestamp" : "2020-05-21T17:18:15.706+0000"
            |}
        """.trimMargin()
            )
        }

        "Equality schema: Valid message alternate date format passes validation." {
            TestUtils.equalityMessageValidator()

            Validator().validate(
                """
            |{
            |   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
            |   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
            |   "@type" : "V4",
            |   "message" : {
            |       "dbObject" : "xxxxxx",
            |       "encryption" : {
            |           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
            |           "encryptedEncryptionKey" : "xxxxxx",
            |           "initialisationVector" : "xxxxxxxx=="
            |       },
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.693",
            |       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_id" : {
            |           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
            |       }
            |   },
            |   "version" : "core-4.release_147.3",
            |   "timestamp" : "2020-05-21T17:18:15.706"
            |}
        """.trimMargin()
            )
        }

        "Equality schema: Valid message alternate date format number two passes validation." {
            TestUtils.equalityMessageValidator()

            Validator().validate(
                """
            |{
            |   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
            |   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
            |   "@type" : "V4",
            |   "message" : {
            |       "dbObject" : "xxxxxx",
            |       "encryption" : {
            |           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
            |           "encryptedEncryptionKey" : "xxxxxx",
            |           "initialisationVector" : "xxxxxxxx=="
            |       },
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.693Z",
            |       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_id" : {
            |           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
            |       }
            |   },
            |   "version" : "core-4.release_147.3",
            |   "timestamp" : "2020-05-21T17:18:15.706Z"
            |}
        """.trimMargin()
            )
        }

        "Equality schema: Valid message - UCFS prod sample 1 passes validation" {
            TestUtils.equalityMessageValidator()

            Validator().validate(
                """
            |{
            |  "traceId" : "59497c68-4e3a-46cd-9215-9e59fe4b22f6",
            |  "unitOfWorkId" : "457e6a1c-c288-497c-a4a2-1e50bef67c90",
            |  "@type" : "V4",
            |  "message" : {
            |    "dbObject" : "xxx",
            |    "encryption" : {
            |      "keyEncryptionKeyId" : "cloudhsm:262152,262151",
            |      "encryptedEncryptionKey" : "xxx",
            |      "encryptionKeyId" : "b4f18de2-86e2-4525-9b39-3130e9a2800f",
            |      "initialisationVector" : "xxxZ"
            |    },
            |    "_lastModifiedDateTime" : "2020-08-05T07:07:39.767+0000",
            |    "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |    "_id" : {
            |      "messageId" : "7b61b79f-80a8-4aed-aa74-212a3c4c4d70"
            |    }
            |  },
            |  "version" : "core-4.release_152.16",
            |  "timestamp" : "2020-08-05T07:07:39.768+0000"
            |}
        """.trimMargin()
            )
        }

        "Equality schema: Valid message - UCFS prod sample 2 passes validation" {
            TestUtils.equalityMessageValidator()

            Validator().validate(
                """
            |{
            |  "traceId" : "1806eb69-a7be-4ade-8306-00f46e6852c5",
            |  "unitOfWorkId" : "ff041e1f-67bb-4013-a00f-3ea787da4864",
            |  "@type" : "V4",
            |  "message" : {
            |    "dbObject" : "xxx",
            |    "encryption" : {
            |      "keyEncryptionKeyId" : "cloudhsm:262152,262151",
            |      "encryptedEncryptionKey" : "xxx",
            |      "encryptionKeyId" : "ff001d09-0da3-408f-a257-fdcd8052bdcd",
            |      "initialisationVector" : "xxx"
            |    },
            |    "_lastModifiedDateTime" : "2020-08-05T07:07:00.105+0000",
            |    "@type" : "EQUALITY_QUESTIONS_NOT_ANSWERED",
            |    "_id" : {
            |      "messageId" : "9e181ca2-2d11-4703-928a-841f7be57c17"
            |    }
            |  },
            |  "version" : "core-4.release_152.16",
            |  "timestamp" : "2020-08-05T07:07:00.105+0000"
            |}
        """.trimMargin()
            )
        }

        "Equality schema: Missing '#/message/@type' field causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
            |{
            |   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
            |   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
            |   "@type" : "V4",
            |   "message" : {
            |       "dbObject" : "xxxxxx",
            |       "encryption" : {
            |           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
            |           "encryptedEncryptionKey" : "xxxxxx",
            |           "initialisationVector" : "xxxxxxxx=="
            |       },
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.693+0000",
            |       "_id" : {
            |           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
            |       }
            |   },
            |   "version" : "core-4.release_147.3",
            |   "timestamp" : "2020-05-21T17:18:15.706+0000"
            |}
            """.trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message: required key [@type] not found'."
        }

        "Equality schema: Incorrect '#/message/@type' type causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
            |{
            |   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
            |   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
            |   "@type" : "V4",
            |   "message" : {
            |       "dbObject" : "xxxxxx",
            |       "encryption" : {
            |           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
            |           "encryptedEncryptionKey" : "xxxxxx",
            |           "initialisationVector" : "xxxxxxxx=="
            |       },
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.693+0000",
            |       "@type" : 1,
            |       "_id" : {
            |           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
            |       }
            |   },
            |   "version" : "core-4.release_147.3",
            |   "timestamp" : "2020-05-21T17:18:15.706+0000"
            |}
            """.trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message/@type: expected type: String, found: Integer'."
        }

        "Equality schema: Empty string '#/message/_id/messageId' field causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
            |{
            |   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
            |   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
            |   "@type" : "V4",
            |   "message" : {
            |       "dbObject" : "xxxxxx",
            |       "encryption" : {
            |           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
            |           "encryptedEncryptionKey" : "xxxxxx",
            |           "initialisationVector" : "xxxxxxxx=="
            |       },
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.693+0000",
            |       "@type" : 1,
            |       "_id" : {
            |           "messageId" : ""
            |       }
            |   },
            |   "version" : "core-4.release_147.3",
            |   "timestamp" : "2020-05-21T17:18:15.706+0000"
            |}
            """.trimMargin()
                )
            }

            exception.message shouldBe "Message failed schema validation: '#/message/@type: expected type: String, found: Integer'."
        }

        "Equality schema: Incorrect '#/message/_id' type causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
            |{
            |   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
            |   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
            |   "@type" : "V4",
            |   "message" : {
            |       "dbObject" : "xxxxxx",
            |       "encryption" : {
            |           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
            |           "encryptedEncryptionKey" : "xxxxxx",
            |           "initialisationVector" : "xxxxxxxx=="
            |       },
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.693+0000",
            |       "@type" : 1,
            |       "_id" : [1, 2, 3, 4, 5, 6, 7 ,8 , 9]
            |   },
            |   "version" : "core-4.release_147.3",
            |   "timestamp" : "2020-05-21T17:18:15.706+0000"
            |}
            """.trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message: 2 schema violations found'."
        }

        "Equality schema: Empty '#/message/_id' type causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
            |{
            |   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
            |   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
            |   "@type" : "V4",
            |   "message" : {
            |       "dbObject" : "xxxxxx",
            |       "encryption" : {
            |           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
            |           "encryptedEncryptionKey" : "xxxxxx",
            |           "initialisationVector" : "xxxxxxxx=="
            |       },
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.693+0000",
            |       "@type" : 1,
            |       "_id" : {}
            |   },
            |   "version" : "core-4.release_147.3",
            |   "timestamp" : "2020-05-21T17:18:15.706+0000"
            |}
            """.trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message: 3 schema violations found'."
        }

        "Equality schema: Missing '#/message/_lastModifiedDateTime' does not cause validation failure." {
            TestUtils.equalityMessageValidator()

            Validator().validate(
                """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : "xxxxxx",
			|       "encryption" : {
			|           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
			|           "encryptedEncryptionKey" : "xxxxxx",
			|           "initialisationVector" : "xxxxxxxx=="
			|       },
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
            )
        }

        "Equality schema: Null '#/message/_lastModifiedDateTime' does not cause validation failure." {
            TestUtils.equalityMessageValidator()

            Validator().validate(
                """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : "xxxxxx",
			|       "encryption" : {
			|           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
			|           "encryptedEncryptionKey" : "xxxxxx",
			|           "initialisationVector" : "xxxxxxxx=="
			|       },
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : null,
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
            )
        }

        "Equality schema: Empty '#/message/_lastModifiedDateTime' does not cause validation failure." {
            TestUtils.equalityMessageValidator()

            Validator().validate(
                """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : "xxxxxx",
			|       "encryption" : {
			|           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
			|           "encryptedEncryptionKey" : "xxxxxx",
			|           "initialisationVector" : "xxxxxxxx=="
			|       },
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : "",
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
            )
        }


        "Equality schema: Incorrect '#/message/_lastModifiedDateTime' type causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : "xxxxxx",
			|       "encryption" : {
			|           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
			|           "encryptedEncryptionKey" : "xxxxxx",
			|           "initialisationVector" : "xxxxxxxx=="
			|       },
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : 42,
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message/_lastModifiedDateTime: #: no subschema matched out of the total 2 subschemas'."
        }

        "Equality schema: Incorrect '#/message/_lastModifiedDateTime' format causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : "xxxxxx",
			|       "encryption" : {
			|           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
			|           "encryptedEncryptionKey" : "xxxxxx",
			|           "initialisationVector" : "xxxxxxxx=="
			|       },
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : "2013-03-13",
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message/_lastModifiedDateTime: #: no subschema matched out of the total 2 subschemas'."
        }

        "Equality schema: Missing '#/message/dbObject' field causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "encryption" : {
			|           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
			|           "encryptedEncryptionKey" : "xxxxxx",
			|           "initialisationVector" : "xxxxxxxx=="
			|       },
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.706+0000",
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message: required key [dbObject] not found'."
        }

        "Equality schema: Incorrect '#/message/dbObject' type  causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : {},
			|       "encryption" : {
			|           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
			|           "encryptedEncryptionKey" : "xxxxxx",
			|           "initialisationVector" : "xxxxxxxx=="
			|       },
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.706+0000",
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message/dbObject: expected type: String, found: JSONObject'."
        }

        "Equality schema: Empty '#/message/dbObject' causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : "",
			|       "encryption" : {
			|           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
			|           "encryptedEncryptionKey" : "xxxxxx",
			|           "initialisationVector" : "xxxxxxxx=="
			|       },
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.706+0000",
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message/dbObject: expected minLength: 1, actual: 0'."
        }

        "Equality schema: Missing '#/message/encryption' causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : "xxxxxx",
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.706+0000",
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message: required key [encryption] not found'."
        }

        "Equality schema: Incorrect '#/message/encryption' type causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : "xxxxxx",
			|       "encryption" : "abc",
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.706+0000",
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message/encryption: expected type: JSONObject, found: String'."
        }

        "Equality schema: Missing keyEncryptionKeyId from '#/message/encryption' type causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : "xxxxxx",
			|       "encryption" : {
			|           "encryptedEncryptionKey" : "xxxxxx",
			|           "initialisationVector" : "xxxxxxxx=="
			|       },
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.706+0000",
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message/encryption: required key [keyEncryptionKeyId] not found'."
        }

        "Equality schema: Missing initialisationVector from '#/message/encryption' type causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : "xxxxxx",
			|       "encryption" : {
			|           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
			|           "encryptedEncryptionKey" : "xxxxxx"
			|       },
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.706+0000",
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message/encryption: required key [initialisationVector] not found'."
        }

        "Equality schema: Missing encryptedEncryptionKey from '#/message/encryption' type causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : "xxxxxx",
			|       "encryption" : {
			|           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
			|           "initialisationVector" : "xxxxxxxx=="
			|       },
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.706+0000",
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message/encryption: required key [encryptedEncryptionKey] not found'."
        }

        "Equality schema: Empty keyEncryptionKeyId from '#/message/encryption' type causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : "xxxxxx",
			|       "encryption" : {
			|           "keyEncryptionKeyId" : "",
			|           "encryptedEncryptionKey" : "xxxxxx",
			|           "initialisationVector" : "xxxxxxxx=="
			|       },
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.706+0000",
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message/encryption/keyEncryptionKeyId: string [] does not match pattern ^cloudhsm:.*$'."
        }

        "Equality schema: Empty initialisationVector from '#/message/encryption' type causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : "xxxxxx",
			|       "encryption" : {
			|           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
			|           "encryptedEncryptionKey" : "xxxxxx",
			|           "initialisationVector" : ""
			|       },
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.706+0000",
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message/encryption/initialisationVector: expected minLength: 1, actual: 0'."
        }

        "Equality schema: Empty encryptedEncryptionKey from '#/message/encryption' type causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : "xxxxxx",
			|       "encryption" : {
			|           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
			|           "encryptedEncryptionKey" : "",
			|           "initialisationVector" : "xxxxxxxx=="
			|       },
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.706+0000",
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message/encryption/encryptedEncryptionKey: expected minLength: 1, actual: 0'."
        }

        "Equality schema: Incorrect '#/message/encryption/keyEncryptionKeyId' type causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : "xxxxxx",
			|       "encryption" : {
			|           "keyEncryptionKeyId" : 42,
			|           "encryptedEncryptionKey" : "xxxxxx",
			|           "initialisationVector" : "xxxxxxxx=="
			|       },
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.706+0000",
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message/encryption/keyEncryptionKeyId: expected type: String, found: Integer'."
        }

        "Equality schema: Incorrect initialisationVector '#/message/encryption/initialisationVector' type causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : "xxxxxx",
			|       "encryption" : {
			|           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
			|           "encryptedEncryptionKey" : "xxxxxx",
			|           "initialisationVector" : {}
			|       },
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.706+0000",
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message/encryption/initialisationVector: expected type: String, found: JSONObject'."
        }

        "Equality schema: Incorrect '#/message/encryption/encryptedEncryptionKey' type causes validation failure." {
            TestUtils.equalityMessageValidator()

            val exception = shouldThrow<InvalidMessageException> {
                Validator().validate(
                    """
			|{
			|   "traceId" : "091f29ab-b6c5-411c-851e-15683ce53c40",
			|   "unitOfWorkId" : "31faa55f-c5e8-4581-8973-383db31ddd77",
			|   "@type" : "V4",
			|   "message" : {
			|       "dbObject" : "xxxxxx",
			|       "encryption" : {
			|           "keyEncryptionKeyId" : "cloudhsm:aaaa,bbbb",
			|           "encryptedEncryptionKey" : ["answer", 42],
			|           "initialisationVector" : "xxxxxxxx=="
			|       },
			|       "@type" : "EQUALITY_QUESTIONS_ANSWERED",
            |       "_lastModifiedDateTime" : "2020-05-21T17:18:15.706+0000",
			|       "_id" : {
			|           "messageId" : "f1d4723b-fdaa-4123-8e20-e6eca6c03645"
			|       }
			|   },
			|   "version" : "core-4.release_147.3",
			|   "timestamp" : "2020-05-21T17:18:15.706+0000"
			|}
			""".trimMargin()
                )
            }
            exception.message shouldBe "Message failed schema validation: '#/message/encryption/encryptedEncryptionKey: expected type: String, found: JSONArray'."
        }
    }
}
