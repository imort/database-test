package io.github.imort.database.main

import android.os.Parcelable
import androidx.compose.runtime.Immutable
import kotlinx.parcelize.Parcelize

@Immutable
@Parcelize
data class MainState(
    val input: String = "",
    val inputError: Boolean = false,
    val logs: List<String> = emptyList(),
) : Parcelable

sealed interface MainAction {
    data class InputChange(val input: String) : MainAction
    data object InputConfirm : MainAction
}

sealed interface MainEffect