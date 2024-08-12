package io.github.imort.database.mvi

import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.compose.LocalLifecycleOwner
import androidx.lifecycle.repeatOnLifecycle
import kotlinx.coroutines.flow.Flow

@Composable
fun <Effect> MviEffect(
    effects: Flow<Effect>,
    lifecycleMinState: Lifecycle.State = Lifecycle.State.STARTED,
    block: suspend (Effect) -> Unit,
) {
    val lifecycleOwner = LocalLifecycleOwner.current
    LaunchedEffect(effects, lifecycleOwner) {
        lifecycleOwner.lifecycle.repeatOnLifecycle(lifecycleMinState) {
            effects.collect(block)
        }
    }
}