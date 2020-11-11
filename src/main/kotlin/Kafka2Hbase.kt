import org.apache.kafka.clients.consumer.KafkaConsumer

fun main() {
    MetadataStoreClient.connect().use { metadataStore ->
        KafkaConsumer<ByteArray, ByteArray>(Config.Kafka.consumerProps).use { kafka ->
            val archiveAwsS3Service = ArchiveAwsS3Service.connect()
            val manifestAwsS3Service = ManifestAwsS3Service.connect()
            Shovel(kafka).shovel(metadataStore, archiveAwsS3Service, manifestAwsS3Service, Config.Kafka.pollTimeout)
        }
    }
}

