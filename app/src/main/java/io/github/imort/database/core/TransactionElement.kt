package io.github.imort.database.core

import kotlin.coroutines.CoroutineContext

/**
 * Support nested withTransaction invocations by branching from current Store/Snapshot
 */
class TransactionElement(var transaction: Transaction?) : CoroutineContext.Element {
    companion object Key : CoroutineContext.Key<TransactionElement>

    override val key: CoroutineContext.Key<*>
        get() = Key
}