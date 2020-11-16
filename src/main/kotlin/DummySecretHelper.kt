import uk.gov.dwp.dataworks.logging.DataworksLogger

class DummySecretHelper: SecretHelperInterface {

    companion object {
        val logger = DataworksLogger.getLogger(DummySecretHelper::class.toString())
    }

    override fun getSecret(secretName: String): String? {

        logger.info("Getting value from dummy secret manager", "secret_name" to secretName)

        try {
            return if (secretName == "password") "password" else getEnv("DUMMY_SECRET_${secretName.toUpperCase()}") ?: "NOT_SET"
        } catch (e: Exception) {
            logger.error("Failed to get dummy secret manager result", e, "secret_name" to secretName)
            throw e
        }
    }
}
