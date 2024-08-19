package io.github.imort.database.core

import io.github.imort.database.DispatchersFactory
import io.github.imort.database.command.Command
import io.github.imort.database.command.Command.Begin
import io.github.imort.database.command.Command.GeneralCommand
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
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

/**
 * Implements database with Snapshot/Read Uncommited isolation level
 * This allows transactions to run in parallel on non intersecting sets of keys
 * Reads and writes are concurrent, commits and rollbacks are synchronized
 * Conflicts are resolving by comparing versions of the data, in such case last transaction rollbacks
 * Nested transactions are supported
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

    private val transactionStack = LinkedList<Transaction>()

    private val CoroutineContext.transactionElement: TransactionElement?
        get() = this[TransactionElement]

    private fun CoroutineContext.transactional(): CoroutineContext = this + when (transactionElement) {
        null -> transactionContext + TransactionElement(transactionStack.peek())
        else -> transactionContext
    }

    private suspend fun <R> withTransactionContext(block: suspend (Store) -> R) =
        withContext(coroutineContext.transactional()) {
            val source = coroutineContext.transactionElement?.transaction?.snapshot ?: store
            block(source)
        }

    private suspend fun currentTransaction() =
        coroutineContext.transactionElement?.transaction ?: transactionStack.peek()

    /**
     * For general clients
     * There is rabbit hole here, will keep things simple using snapshots of the store
     * @see <a href="https://medium.com/androiddevelopers/threading-models-in-coroutines-and-android-sqlite-api-6cab11f7eb90">Room developers article</a>
     */
    suspend fun withTransaction(block: suspend TransactionScope.() -> Unit): Unit = withTransactionContext { source ->
        executeInNewTransaction(source, block)
    }

    /**
     * For interpreter clients
     */
    suspend fun execute(command: Command): String = withTransactionContext { source ->
        when (command) {
            Begin -> {
                val transaction = beginTransaction(source)
                transactionStack.push(transaction)
                ""
            }

            is GeneralCommand -> {
                val transaction = currentTransaction()
                if (transaction == null) {
                    when (command) {
                        Commit, Rollback -> "No transaction".also { logsChannel.trySend(it) }
                        else -> executeInNewTransaction(source) {
                            val r = perform(command)
                            commit()
                            return@executeInNewTransaction r
                        }
                    }
                } else {
                    val result = transaction.scope.perform(command)
                    if (command is Commit || command is Rollback) {
                        if (transaction == transactionStack.peek()) {
                            endTransaction(transaction)
                            transactionStack.pop()
                        } else {
                            error("Ending transaction started by withTransaction")
                        }
                    }
                    result
                }
            }
        }
    }

    private suspend fun <R> executeInNewTransaction(source: Store, block: suspend TransactionScope.() -> R): R {
        val transaction = beginTransaction(source)
        try {
            return withContext(parallelContext) {
                block.invoke(transaction.scope)
            }
        } finally {
            endTransaction(transaction)
        }
    }

    private suspend fun beginTransaction(source: Store): Transaction {
        val transaction = Transaction(logsChannel, source.snapshot())
        Timber.i("beginTransaction ${transaction.id} stack ${transactionStack.size} on ${Thread.currentThread()}")
        val element = coroutineContext.transactionElement ?: error("Should be available here")
        element.transaction = transaction
        return transaction
    }

    private fun endTransaction(transaction: Transaction) {
        Timber.i("endTransaction ${transaction.id} stack ${transactionStack.size} on ${Thread.currentThread()}")
        if (transaction.successful) transaction.snapshot.commit()
    }
}