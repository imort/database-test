package io.github.imort.database.store

import io.github.imort.database.store.Store.StoreValue
import timber.log.Timber
import javax.inject.Inject
import javax.inject.Singleton

interface Store<T> {
    fun keys(): Set<String>
    fun version(key: String): Int?
    fun get(key: String): T?
    fun set(key: String, value: T?)

    fun snapshot(): Snapshot<T> = Snapshot(this)

    fun merge(changes: Map<String, Snapshot.Change<T>>) {
        changes.keys.forEach { key ->
            val targetVersion = changes[key]?.version ?: error("Missing changes for $key")
            val sourceVersion = version(key) ?: return@forEach
            check(targetVersion == sourceVersion) { "Conflict on $key" }
        }
        changes.forEach { (key, change) ->
            set(key, change.value)
        }
    }

    data class StoreValue<T>(val value: T, val version: Int)
}

@Singleton
class StoreImpl @Inject constructor() : Store<String> {
    private val data = mutableMapOf<String, StoreValue<String>>()

    override fun keys(): Set<String> = data.keys

    override fun version(key: String): Int? = data[key]?.version

    override fun get(key: String): String? {
        Timber.d("GET $key on ${Thread.currentThread()}")
        return data[key]?.value
    }

    override fun set(key: String, value: String?) {
        if (value == null) {
            Timber.d("REMOVE $key on ${Thread.currentThread()}")
            data.remove(key)
        } else {
            Timber.d("SET $key:$value on ${Thread.currentThread()}")
            val version = data[key]?.version?.let { it + 1 } ?: 0
            data[key] = StoreValue(value, version)
        }
    }
}