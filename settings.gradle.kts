rootProject.name = "kafka2hbase"

buildCache {
    local<DirectoryBuildCache>{
        setDirectory(File(settingsDir, "build-cache"))
    }
}