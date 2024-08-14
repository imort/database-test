package io.github.imort.database.core

import io.github.imort.database.DispatchersFactory
import io.github.imort.database.command.Command
import io.github.imort.database.command.Command.GeneralCommand.Commit
import io.github.imort.database.command.Command.GeneralCommand.Rollback
import io.github.imort.database.core.Transaction.TransactionScope
import io.github.imort.database.store.Store
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.withContext
import timber.log.Timber
import java.util.LinkedList
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Implements ACID database with Snapshot isolation level
 * This allows transactions to run in parallel on non intersecting sets of keys
 * Reads and writes are concurrent, commits and rollbacks are synchronized
 * Conflicts are resolving by comparing versions of the data, in such case last transaction rollbacks
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
    private val transactionDispatcher = dispatchersFactory.database

    private val transactionStack = LinkedList<Transaction>()
    private val currentTransaction: Transaction?
        get() = transactionStack.peek()

    /**
     * Proper implementation is out of scope
     * This is why TransactionScope offers blocking interface
     * @see <a href="https://medium.com/androiddevelopers/threading-models-in-coroutines-and-android-sqlite-api-6cab11f7eb90">Room developers article</a>
     */
    suspend fun withTransaction(block: suspend TransactionScope.() -> Unit): Unit = withContext(transactionDispatcher) {
        executeInNewTransaction(block)
    }

    // For interpreter clients
    suspend fun execute(command: Command): String = withContext(transactionDispatcher) {
        when (command) {
            is Command.Begin -> {
                beginTransaction()
                ""
            }

            is Command.GeneralCommand -> {
                val transaction = currentTransaction
                if (transaction == null) {
                    when (command) {
                        Commit, Rollback -> "No transaction"
                        else -> executeInNewTransaction {
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
                        Commit, Rollback -> endTransaction(transaction)
                        else -> Unit
                    }
                    r
                }
            }
        }
    }

    private fun beginTransaction(): Transaction {
        val snapshot = when (val current = currentTransaction) {
            null -> store.snapshot()
            else -> current.snapshot.snapshot()
        }
        return Transaction(logsChannel, snapshot).also {
            transactionStack.push(it)
            Timber.i("beginTransaction stack ${transactionStack.size}")
        }
    }

    private suspend fun endTransaction(transaction: Transaction) {
        check(transaction === transactionStack.pop()) { "Ending transaction not on top of the stack" }
        if (transaction.successful) transaction.snapshot.commit()
        Timber.i("endTransaction stack ${transactionStack.size}")
    }

    private suspend fun <R> executeInNewTransaction(block: suspend TransactionScope.() -> R): R {
        val transaction = beginTransaction()
        try {
            // Wrap suspending block in a new scope to wait for any child coroutine.
            val result = coroutineScope {
                block.invoke(transaction.scope)
            }
            return result
        } finally {
            endTransaction(transaction)
        }
    }
}