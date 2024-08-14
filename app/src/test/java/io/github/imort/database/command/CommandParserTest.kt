package io.github.imort.database.command

import io.github.imort.database.command.Command.Begin
import io.github.imort.database.command.Command.GeneralCommand.Commit
import io.github.imort.database.command.Command.GeneralCommand.Count
import io.github.imort.database.command.Command.GeneralCommand.Delete
import io.github.imort.database.command.Command.GeneralCommand.Get
import io.github.imort.database.command.Command.GeneralCommand.Rollback
import io.github.imort.database.command.Command.GeneralCommand.Set
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.junit.Test

class CommandParserTest {
    @Test
    fun parse(): Unit = with(CommandParser) {
        shouldThrow<IllegalArgumentException> {
            parse("")
        }
        shouldThrow<IllegalArgumentException> {
            parse("unknown arg arg")
        }

        parse("get aRg1") shouldBe Get("aRg1")
        parse(" gEt  aRg1   ") shouldBe Get("aRg1")
        shouldThrow<IllegalArgumentException> {
            parse("get")
        }

        parse("count aRg1") shouldBe Count("aRg1")
        parse("  coUnt          aRg1 ") shouldBe Count("aRg1")
        shouldThrow<IllegalArgumentException> {
            parse("count")
        }

        parse("set aRg1 ArG2") shouldBe Set("aRg1", "ArG2")
        parse(" sEt     aRg1    ArG2   ") shouldBe Set("aRg1", "ArG2")
        shouldThrow<IllegalArgumentException> {
            parse("set")
        }

        parse("delete aRg1") shouldBe Delete("aRg1")
        shouldThrow<IllegalArgumentException> {
            parse("delete")
        }

        parse("begin") shouldBe Begin
        parse(" bEgIn whatever ") shouldBe Begin

        parse("commit") shouldBe Commit
        parse(" cOmMiT wiTh     snapshot ") shouldBe Commit

        parse("rollback") shouldBe Rollback
        parse("  rollBaCk           kindly   ") shouldBe Rollback
    }
}