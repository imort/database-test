package io.github.imort.database.core

import io.github.imort.database.DispatchersFactory
import io.github.imort.database.command.Command
import io.github.imort.database.command.Command.GeneralCommand.Commit
import io.github.imort.database.command.Command.GeneralCommand.Rollback
import io.github.imort.database.core.Transaction.TransactionScope
import io.github.imort.database.store.Store
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.withContext
import timber.log.Timber
import java.util.LinkedList
import java.util.concurrent.atomic.AtomicBoolean
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Implements ACID database with Snapshot isolation level
 * This allows transactions to run in parallel on non intersecting sets of keys
 * Reads and writes are concurrent, commits and rollbacks are synchronized
 * Conflicts are resolving by comparing versions of the data, in such case last transaction rollbacks
 * Nested transactions are supported for interpreters on the single thread
 */
@Singleton
class Database @Inject constructor(
    dispatchersFactory: DispatchersFactory,
    private val store: Store,
) {

    private val logsChannel = Channel<String>()
    val logsFlow = logsChannel.receiveAsFlow()

    /**
     * Usually databases require operations to execute on the single thread.
     * Snapshot isolation does not prevent suspend functions inside transaction
     * to change dispatcher and get higher parallelism.
     */
    private val transactionContext = dispatchersFactory.database + CoroutineName("db-transaction")
    private val parallelContext = dispatchersFactory.default + CoroutineName("db-parallel")

    private val transactionStackLocalInitialized = AtomicBoolean(false)
    private val transactionStackLocal = ThreadLocal.withInitial {
        if (transactionStackLocalInitialized.getAndSet(true)) error("Interpreter allowed only on single thread")
        LinkedList<Transaction>()
    }
    private val transactionStack
        get() = transactionStackLocal.get() ?: error("Should fail in initializer")
    private val currentTransaction: Transaction?
        get() = transactionStack.peek()

    /**
     * For general clients, multithreading support
     * There is rabbit hole here, will keep things simple using snapshots of the store
     * @see <a href="https://medium.com/androiddevelopers/threading-models-in-coroutines-and-android-sqlite-api-6cab11f7eb90">Room developers article</a>
     */
    suspend fun withTransaction(block: suspend TransactionScope.() -> Unit): Unit = withContext(transactionContext) {
        executeInNewTransaction(interpreter = false, block)
    }

    /**
     * For interpreter clients, nested transactions support
     */
    suspend fun execute(command: Command): String {
        return withContext(transactionContext) {
            when (command) {
                is Command.Begin -> {
                    beginTransaction(interpreter = true)
                    ""
                }

                is Command.GeneralCommand -> {
                    val transaction = currentTransaction
                    if (transaction == null) {
                        when (command) {
                            Commit, Rollback -> "No transaction".also { logsChannel.trySend(it) }
                            else -> executeInNewTransaction(interpreter = true) {
                                val r = perform(command)
                                commit()
                                return@executeInNewTransaction r
                            }
                        }
                    } else {
                        val r = try {
                            transaction.scope.perform(command)
                        } catch (t: Throwable) {
                            t.message ?: "Unknown error"
                        }
                        when (command) {
                            Commit, Rollback -> endTransaction(transaction, interpreter = true)
                            else -> Unit
                        }
                        r
                    }
                }
            }
        }
    }

    private fun beginTransaction(interpreter: Boolean): Transaction {
        val snapshot = if (interpreter) {
            when (val current = currentTransaction) {
                null -> store.snapshot()
                else -> current.snapshot.snapshot()
            }
        } else {
            store.snapshot()
        }
        return Transaction(logsChannel, snapshot).also { transaction ->
            if (interpreter) {
                transactionStack.push(transaction)
                Timber.i(
                    "beginTransaction ${transaction.id} stack ${transactionStack.size} on ${Thread.currentThread()}",
                )
            } else {
                Timber.i("beginTransaction ${transaction.id} on ${Thread.currentThread()}")
            }
        }
    }

    private fun endTransaction(transaction: Transaction, interpreter: Boolean) {
        if (interpreter) {
            check(transaction === transactionStack.pop()) { "Ending transaction not on top of the stack" }
        }
        if (transaction.successful) transaction.snapshot.commit()
        Timber.i("endTransaction ${transaction.id} stack ${transactionStack.size} on ${Thread.currentThread()}")
    }

    private suspend fun <R> executeInNewTransaction(interpreter: Boolean, block: suspend TransactionScope.() -> R): R {
        val transaction = beginTransaction(interpreter)
        try {
            return withContext(parallelContext) {
                block.invoke(transaction.scope)
            }
        } finally {
            endTransaction(transaction, interpreter)
        }
    }
}