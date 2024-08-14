package io.github.imort.database.main

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.imePadding
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.Send
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TextField
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.tooling.preview.Preview
import androidx.compose.ui.unit.dp
import androidx.hilt.navigation.compose.hiltViewModel
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import io.github.imort.database.ui.theme.DatabaseTheme
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow

@Composable
fun MainScreen(
    modifier: Modifier = Modifier,
    vm: MainViewModel = hiltViewModel(),
) {
    val state by vm.states.collectAsStateWithLifecycle()
    MainScreenImpl(
        modifier = modifier,
        state = state,
        action = vm::action,
        effects = vm.effects,
    )
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun MainScreenImpl(
    modifier: Modifier = Modifier,
    state: MainState = MainState(),
    action: (MainAction) -> Unit = {},
    @Suppress("UNUSED_PARAMETER") effects: Flow<MainEffect> = emptyFlow(),
) {
    Scaffold(
        modifier = modifier
            .fillMaxSize()
            .imePadding(),
        topBar = {
            TopAppBar(title = { Text(text = "Database interpreter") })
        },
    ) { innerPadding ->
        Column(modifier = Modifier.padding(innerPadding)) {
            val lazyState = rememberLazyListState()
            LazyColumn(
                modifier = Modifier
                    .weight(1f)
                    .padding(horizontal = 16.dp),
                state = lazyState,
                reverseLayout = true,
            ) {
                items(state.logs.reversed()) { log ->
                    Text(text = log, modifier = Modifier.fillMaxWidth())
                }
            }
            LaunchedEffect(key1 = state.logs.size) {
                lazyState.animateScrollToItem(0)
            }

            TextField(
                value = state.input,
                onValueChange = { action(MainAction.InputChange(it)) },
                modifier = Modifier.fillMaxWidth(),
                trailingIcon = {
                    IconButton(onClick = { action(MainAction.InputConfirm) }) {
                        Icon(imageVector = Icons.AutoMirrored.Filled.Send, contentDescription = "Execute")
                    }
                },
                isError = state.inputError,
                keyboardOptions = KeyboardOptions.Default.copy(
                    keyboardType = KeyboardType.Text,
                    imeAction = ImeAction.Go,
                ),
                keyboardActions = KeyboardActions(
                    onGo = { action(MainAction.InputConfirm) },
                ),
                singleLine = true,
            )
        }
    }
}

@Preview(showBackground = true)
@Composable
fun GreetingPreview() {
    DatabaseTheme {
        MainScreenImpl(state = MainState(input = "command", logs = listOf("one", "two", "three")))
    }
}