rootProject.name = "kafka2hbase"

buildCache {
    local<DirectoryBuildCache>{
        directory = File(settingsDir, "build-cache")
    }
}
