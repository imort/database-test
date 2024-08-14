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
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class DatabaseTest {

    companion object {
        @get:ClassRule
        @JvmStatic
        val timberRule = TimberRule()
    }

    @get:Rule
    val testDispatcherRule = TestDispatcherRule()

    private val db = Database(
        dispatchersFactory = DispatchersFactory.DefaultDispatchersFactory,
        store = StoreImpl(),
    )

    private fun TestScope.collectLogs(database: Database): MutableList<String> {
        val logs = mutableListOf<String>()
        backgroundScope.launch(UnconfinedTestDispatcher()) {
            database.logsFlow.toList(logs)
        }
        return logs
    }

    @Test
    fun setAndGetValue() = runTest {
        with(db) {
            execute(Set("foo", "123"))
            execute(Get("foo")) shouldBe "123"
        }
    }

    @Test
    fun deleteValue() = runTest {
        with(db) {
            execute(Set("foo", "123"))
            execute(Delete("foo"))
            execute(Get("foo")) shouldBe "Key foo not set"
        }
    }

    @Test
    fun countValues() = runTest {
        with(db) {
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
        with(db) {
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
        with(db) {
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
        with(db) {
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
        with(db) {
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
    fun nestedTransactions() = runTest {
        with(db) {
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
    fun manySequentialTransactions() = runTest {
        with(db) {
            for (i in 0 until 100) {
                withTransaction {
                    set(i.toString(), "abc")
                    commit()
                }
            }
            execute(Count("abc")) shouldBe "100"
        }
    }

    @Test
    fun manyParallelTransactions() = runTest {
        with(db) {
            val jobs = (0 until 100).map {
                val key = it.toString()
                backgroundScope.launch {
                    withTransaction {
                        set(key, "123")
                        delete(key)
                        set(key, "abc")
                        commit()
                    }
                }
            }
            jobs.joinAll()
            execute(Count("abc")) shouldBe "100"
        }
    }

    @Test
    fun conflictAvoided() = runTest {
        db.execute(Set("foo", "123"))
        db.execute(Set("bar", "456"))

        val job1 = backgroundScope.launch {
            db.withTransaction {
                set("foo", "456")
                commit()
            }
        }
        val job2 = backgroundScope.launch {
            db.withTransaction {
                set("bar", "123")
                commit()
            }
        }
        joinAll(job1, job2)

        db.execute(Get("foo")) shouldBe "456"
        db.execute(Get("bar")) shouldBe "123"
    }

    @Test
    fun conflictOccurred() = runTest {
        db.execute(Set("foo", "123"))

        val mutex1 = Mutex(locked = true)
        val job1 = backgroundScope.launch {
            db.withTransaction {
                set("foo", "456")
                mutex1.withLock {
                    commit()
                }
            }
        }
        mutex1.unlock()
        job1.join()

        val mutex2 = Mutex(locked = true)
        val job2 = backgroundScope.launch {
            db.withTransaction {
                set("foo", "789")
                mutex2.withLock {
                    commit()
                }
            }
        }
        mutex2.unlock()
        job2.join()

        db.execute(Get("foo")) shouldBe "789"
    }
}