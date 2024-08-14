package io.github.imort.database

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.newSingleThreadContext
import kotlin.coroutines.CoroutineContext

interface DispatchersFactory {
    val database: CoroutineContext
    val default: CoroutineContext
    val io: CoroutineContext

    companion object DefaultDispatchersFactory : DispatchersFactory {
        @OptIn(ExperimentalCoroutinesApi::class, DelicateCoroutinesApi::class)
        override val database: CoroutineDispatcher = newSingleThreadContext("db")
        override val default: CoroutineDispatcher = Dispatchers.Default
        override val io: CoroutineDispatcher = Dispatchers.IO
    }
}