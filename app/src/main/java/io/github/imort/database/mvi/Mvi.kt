package io.github.imort.database.mvi

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow

interface Mvi<Action, State, Effect> {
    val states: StateFlow<State>
    val effects: Flow<Effect>
    fun action(action: Action)
}