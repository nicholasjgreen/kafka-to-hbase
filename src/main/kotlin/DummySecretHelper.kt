class DummySecretHelper: SecretHelperInterface {

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(DummySecretHelper::class.toString())
    }

    override fun getSecret(secretName: String): String? {

        logger.info("Getting value from dummy secret manager", "secret_name", secretName)

        try {
            return getEnv("DUMMY_SECRET_${secretName.toUpperCase()}") ?: "NOT_SET"
        } catch (e: Exception) {
            logger.error("Failed to get dummy secret manager result", e, "secret_name", secretName)
            throw e
        }
    }
}
