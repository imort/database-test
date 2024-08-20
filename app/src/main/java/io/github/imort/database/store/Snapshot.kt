package io.github.imort.database.store

import timber.log.Timber

class Snapshot<T>(private val store: Store<T>) : Store<T> {
    private val changes = mutableMapOf<String, Change<T>>()

    override fun keys(): Set<String> {
        return store.keys() + changes.keys
    }

    override fun version(key: String): Int? {
        return changes[key]?.version ?: store.version(key)
    }

    fun count(value: String): Int {
        return keys().map { get(it) }.count { it == value }
    }

    override fun get(key: String): T? {
        Timber.v("get $key on ${Thread.currentThread()}")
        val change = changes[key] ?: return store.get(key)
        return change.value
    }

    override fun set(key: String, value: T?) {
        Timber.v("set $key:$value on ${Thread.currentThread()}")
        changes[key] = Change(value, store.version(key) ?: 0)
    }

    fun commit() {
        Timber.i("commit on ${Thread.currentThread()}")
        store.merge(changes)
    }

    // null value marks deletion
    data class Change<T>(val value: T?, val version: Int)
}