package io.github.imort.database.core

import io.github.imort.TestDispatcherRule
import io.github.imort.TimberRule
import io.github.imort.database.DispatchersFactory
import io.github.imort.database.command.Command.Begin
import io.github.imort.database.command.Command.GeneralCommand.Commit
import io.github.imort.database.command.Command.GeneralCommand.Count
import io.github.imort.database.command.Command.GeneralCommand.Delete
import io.github.imort.database.command.Command.GeneralCommand.Get
import io.github.imort.database.command.Command.GeneralCommand.Rollback
import io.github.imort.database.command.Command.GeneralCommand.Set
import io.github.imort.database.store.StoreImpl
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.test.runTest
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test

class DatabaseTest {

    companion object {
        @get:ClassRule
        @JvmStatic
        val timberRule = TimberRule()
    }

    @get:Rule
    val testDispatcherRule = TestDispatcherRule()
    private val dispatchersFactory = object : DispatchersFactory {
        override val database = testDispatcherRule.testDispatcher
        override val default = testDispatcherRule.testDispatcher
        override val io = testDispatcherRule.testDispatcher
    }

    private val database = Database(
        dispatchersFactory = dispatchersFactory,
        store = StoreImpl(dispatchersFactory),
    )

    @Test
    fun setAndGetValue() = runTest {
        with(database) {
            execute(Set("foo", "123"))
            execute(Get("foo")) shouldBe "123"
        }
    }

    @Test
    fun deleteValue() = runTest {
        with(database) {
            execute(Set("foo", "123"))
            execute(Delete("foo"))
            execute(Get("foo")) shouldBe "Key foo not set"
        }
    }

    @Test
    fun countValues() = runTest {
        with(database) {
            execute(Set("foo", "123"))
            execute(Set("bar", "456"))
            execute(Set("baz", "123"))
            execute(Count("123")) shouldBe "2"
            execute(Count("456")) shouldBe "1"
            execute(Count("789")) shouldBe "0"
        }
    }

    @Test
    fun commitTransaction1() = runTest {
        with(database) {
            execute(Set("bar", "123"))
            execute(Get("bar")) shouldBe "123"
            execute(Begin) shouldBe ""
            execute(Set("foo", "456"))
            execute(Get("bar")) shouldBe "123"
            execute(Delete("bar"))
            execute(Commit) shouldBe ""
            execute(Get("bar")) shouldBe "Key bar not set"
            execute(Commit) shouldBe "No transaction"
            execute(Get("foo")) shouldBe "456"
        }
    }

    @Test
    fun commitTransaction2() = runTest {
        with(database) {
            execute(Set("bar", "123"))
            execute(Get("bar")) shouldBe "123"
            withTransaction {
                set("foo", "456")
                get("bar") shouldBe "123"
                delete("bar")
                commit()
            }
            execute(Get("bar")) shouldBe "Key bar not set"
            execute(Commit) shouldBe "No transaction"
            execute(Get("foo")) shouldBe "456"
        }
    }

    @Test
    fun rollbackTransaction1() = runTest {
        with(database) {
            execute(Set("foo", "123"))
            execute(Set("bar", "abc"))
            execute(Begin) shouldBe ""
            execute(Set("foo", "456"))
            execute(Get("foo")) shouldBe "456"
            execute(Set("bar", "def"))
            execute(Get("bar")) shouldBe "def"
            execute(Rollback) shouldBe ""
            execute(Get("foo")) shouldBe "123"
            execute(Get("bar")) shouldBe "abc"
            execute(Rollback) shouldBe "No transaction"
        }
    }

    @Test
    fun rollbackTransaction2() = runTest {
        with(database) {
            execute(Set("foo", "123"))
            execute(Set("bar", "abc"))
            withTransaction {
                set("foo", "456")
                get("foo") shouldBe "456"
                set("bar", "def")
                get("bar") shouldBe "def"
                rollback()
            }
            execute(Get("foo")) shouldBe "123"
            execute(Get("bar")) shouldBe "abc"
            execute(Rollback) shouldBe "No transaction"
        }
    }

    @Test
    fun nestedTransactions1() = runTest {
        with(database) {
            execute(Set("foo", "123"))
            execute(Set("bar", "456"))
            execute(Begin) shouldBe ""
            execute(Set("foo", "456"))
            execute(Begin) shouldBe ""
            execute(Count("456")) shouldBe "2"
            execute(Get("foo")) shouldBe "456"
            execute(Set("foo", "789"))
            execute(Get("foo")) shouldBe "789"
            execute(Rollback) shouldBe ""
            execute(Get("foo")) shouldBe "456"
            execute(Delete("foo"))
            execute(Get("foo")) shouldBe "Key foo not set"
            execute(Rollback) shouldBe ""
            execute(Get("foo")) shouldBe "123"
        }
    }

    @Test
    fun nestedTransactions2() = runTest {
        with(database) {
            execute(Set("foo", "123"))
            execute(Set("bar", "456"))
            withTransaction {
                set("foo", "456")
                withTransaction {
                    count("456") shouldBe "2"
                    get("foo") shouldBe "456"
                    set("foo", "789")
                    get("foo") shouldBe "789"
                    rollback()
                }
                get("foo") shouldBe "456"
                delete("foo")
                get("foo") shouldBe "Key foo not set"
                rollback() shouldBe ""
            }
            execute(Get("foo")) shouldBe "123"
        }
    }
}