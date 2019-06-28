rootProject.name = "kafka2hbase"

buildCache {
    local<DirectoryBuildCache>{
        setDirectory(File(settingsDir, "build-cache"))
        println(settingsDir)
        println(directory)
    }
}