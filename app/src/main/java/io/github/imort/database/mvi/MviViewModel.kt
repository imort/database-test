package io.github.imort.database.mvi

import android.os.Parcelable
import androidx.annotation.MainThread
import androidx.lifecycle.SavedStateHandle
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import timber.log.Timber

abstract class MviViewModel<Action, State : Parcelable, Effect>(
    private val savedStateHandle: SavedStateHandle,
    initialState: State,
) : ViewModel(), Mvi<Action, State, Effect> {

    override val states = savedStateHandle.getStateFlow(STATE, initialState)
    val state: State
        @MainThread
        get() = states.value

    private val _effects = Channel<Effect> {
        Timber.w("$it is undelivered")
    }
    override val effects = _effects.receiveAsFlow()

    @MainThread
    final override fun action(action: Action) {
        viewModelScope.launch {
            handle(action)
        }
    }

    @MainThread
    protected abstract suspend fun handle(action: Action)

    @MainThread
    protected fun update(block: State.() -> State) {
        savedStateHandle[STATE] = block(states.value)
    }

    protected fun send(effect: Effect) {
        viewModelScope.launch {
            _effects.send(effect)
        }
    }

    companion object {
        private const val STATE = "state"
    }
}