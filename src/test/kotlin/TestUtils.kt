import java.util.*

class TestUtils {

    companion object {
        fun defaultMessageValidator(){
            Config.Validator.properties = Properties().apply {
                put(Config.schemaFileProperty, Config.mainSchemaFile)
            }
        }

        fun equalityMessageValidator(){
            Config.Validator.properties = Properties().apply {
                put(Config.schemaFileProperty, Config.equalitySchemaFile)
            }
        }
    }

}
