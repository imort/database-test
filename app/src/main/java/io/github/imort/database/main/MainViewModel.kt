package io.github.imort.database.main

import androidx.lifecycle.SavedStateHandle
import androidx.lifecycle.viewModelScope
import dagger.hilt.android.lifecycle.HiltViewModel
import io.github.imort.database.command.CommandParser
import io.github.imort.database.core.Database
import io.github.imort.database.mvi.MviViewModel
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import javax.inject.Inject

@HiltViewModel
class MainViewModel @Inject constructor(
    savedStateHandle: SavedStateHandle,
    private val database: Database,
) : MviViewModel<MainAction, MainState, MainEffect>(savedStateHandle, MainState()) {

    init {
        database.logsFlow
            .onEach {
                update { copy(logs = logs + it) }
            }
            .launchIn(viewModelScope)
    }

    override suspend fun handle(action: MainAction) = when (action) {
        is MainAction.InputChange -> update { copy(input = action.input, inputError = false) }
        is MainAction.InputConfirm -> executeWithDatabase()
    }

    private suspend fun executeWithDatabase() {
        val input = state.input
        val command = try {
            CommandParser.parse(input)
        } catch (t: Throwable) {
            val error = t.message ?: "Unknown error"
            update { copy(logs = logs + error) }
            return
        }
        update { copy(input = "", logs = logs + "> $input") }
        database.execute(command)
    }
}