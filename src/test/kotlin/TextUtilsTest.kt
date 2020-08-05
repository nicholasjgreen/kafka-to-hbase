import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.StringSpec

class TextUtilsTest : StringSpec({

    fun reset() {
        Config.Hbase.qualifiedTablePattern = """^\w+\.([-\w]+)\.([-\w]+)$"""
    }

    "agent_core:agentToDoArchive is coalesced." {
        val actual = TextUtils().coalescedName("agent_core:agentToDoArchive")
        actual shouldBe "agent_core:agentToDo"
    }

    "other_db:agentToDoArchive is not coalesced." {
        val actual = TextUtils().coalescedName("other_db:agentToDoArchive")
        actual shouldBe "other_db:agentToDoArchive"
    }

    "Not agentToDoArchive is not coalesced." {
        val actual = TextUtils().coalescedName("core:calculationParts")
        actual shouldBe "core:calculationParts"
    }

    "Test topic name table matcher will use ucfs data feed regex to match against valid table name" {

        val tableName = "db.ucfs.data"

        Config.Hbase.qualifiedTablePattern = """^\w+\.([-\w]+)\.([-\w]+)$"""

        val result = TextUtils().topicNameTableMatcher(tableName)

        result shouldNotBe null

        assert(result!!.groupValues[1] == "ucfs")
        assert(result.groupValues[2] == "data")

        reset()
    }

    "Test topic name table matcher will use data equalities regex to match against valid table name" {

        val tableName = "data.equalities"

        Config.Hbase.qualifiedTablePattern = """([-\w]+)\.([-\w]+)"""

        val result: MatchResult? = TextUtils().topicNameTableMatcher(tableName)

        result shouldNotBe null

        assert(result!!.groupValues[1] == "data")
        assert(result.groupValues[2] == "equalities")

        reset()
    }
})
