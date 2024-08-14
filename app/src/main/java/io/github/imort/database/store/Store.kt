package io.github.imort.database.store

import io.github.imort.database.DispatchersFactory
import io.github.imort.database.store.Store.StoreValue
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import timber.log.Timber
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.random.Random

interface Store {
    suspend fun keys(): Set<String>
    suspend fun version(key: String): Int?
    suspend fun get(key: String): String?
    suspend fun set(key: String, value: String?)

    fun snapshot() = Snapshot(this)

    suspend fun merge(changes: Map<String, Snapshot.Change>) {
        changes.keys.forEach { key ->
            val targetVersion = changes[key]!!.version
            val sourceVersion = version(key) ?: return@forEach
            check(targetVersion == sourceVersion) { "Conflict on $key" }
        }
        changes.forEach { (key, change) ->
            set(key, change.value)
        }
    }

    data class StoreValue(val value: String, val version: Int)
}

@Singleton
class StoreImpl @Inject constructor(dispatchersFactory: DispatchersFactory) : Store {
    // Emulate hard work on io
    private val ioDispatcher = dispatchersFactory.io
    private val data = mutableMapOf<String, StoreValue>()

    override suspend fun keys(): Set<String> = withContext(ioDispatcher) {
        randomDelay()
        data.keys.toSet()
    }

    override suspend fun version(key: String): Int? = withContext(ioDispatcher) {
        data[key]?.version
    }

    override suspend fun get(key: String) = withContext(ioDispatcher) {
        randomDelay()
        Timber.d("GET $key on ${Thread.currentThread()}")
        data[key]?.value
    }

    override suspend fun set(key: String, value: String?): Unit = withContext(ioDispatcher) {
        randomDelay()
        if (value == null) {
            Timber.d("REMOVE $key on ${Thread.currentThread()}")
            data.remove(key)
        } else {
            Timber.d("SET $key:$value on ${Thread.currentThread()}")
            val version = data[key]?.version?.let { it + 1 } ?: 0
            data[key] = StoreValue(value, version)
        }
    }

    private suspend fun randomDelay(until: Long = 10) = delay(Random.nextLong(until) + 1)
}