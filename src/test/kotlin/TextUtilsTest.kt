import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec

class TextUtilsTest : StringSpec({


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

})
