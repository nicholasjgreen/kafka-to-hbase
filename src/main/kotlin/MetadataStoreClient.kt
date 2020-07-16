import java.sql.Connection
import java.sql.DriverManager
import java.util.*

open class MetadataStoreClient(var connection: Connection) {

    companion object {

        private val isUsingAWS = Config.MetadataStore.isUsingAWS
        private val secretHelper: SecretHelperInterface =  if (isUsingAWS) AWSSecretHelper() else DummySecretHelper()

        fun connect(): MetadataStoreClient {

            val hostname = Config.MetadataStore.properties["rds.endpoint"]
            val port = Config.MetadataStore.properties["rds.port"]
            val jdbcUrl = "jdbc:mysql://$hostname:$port/"
            val username = Config.MetadataStore.properties.getProperty("user")
            val secretName = Config.MetadataStore.properties.getProperty("rds.password.secret.name")

            logger.info("Connecting to RDS Metadata Store", "jdbc_url", jdbcUrl, "username", username)

            val propertiesWithPassword: Properties = Config.MetadataStore.properties.clone() as Properties

            propertiesWithPassword["password"] = secretHelper.getSecret(secretName)

            return MetadataStoreClient(DriverManager.getConnection(jdbcUrl, propertiesWithPassword))
        }

        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(MetadataStoreClient::class.toString())
    }

    fun close() = connection.close()
}
