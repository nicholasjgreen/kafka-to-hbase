rootProject.name = "kafka2hbase"

buildCache {
    local{
        directory = File(settingsDir, "build-cache")
    }
}
