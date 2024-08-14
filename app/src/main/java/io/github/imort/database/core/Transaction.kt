package io.github.imort.database.core

import io.github.imort.database.command.Command
import io.github.imort.database.command.Command.GeneralCommand.Commit
import io.github.imort.database.command.Command.GeneralCommand.Count
import io.github.imort.database.command.Command.GeneralCommand.Delete
import io.github.imort.database.command.Command.GeneralCommand.Get
import io.github.imort.database.command.Command.GeneralCommand.Rollback
import io.github.imort.database.command.Command.GeneralCommand.Set
import io.github.imort.database.store.Snapshot
import kotlinx.coroutines.channels.Channel

class Transaction(
    val logger: Channel<String>,
    val snapshot: Snapshot,
    var successful: Boolean = false,
) {
    val scope = TransactionScope()

    inner class TransactionScope {
        suspend fun perform(command: Command.GeneralCommand): String = when (command) {
            is Get -> get(command.key)
            is Count -> count(command.value)
            is Set -> set(command.key, command.value)
            is Delete -> delete(command.key)
            is Commit -> commit()
            is Rollback -> rollback()
        }

        suspend fun get(key: String): String {
            val value = snapshot.get(key)
            logger.send(value ?: "Key $key not set")
            return value ?: "Key $key not set"
        }

        suspend fun count(value: String): String {
            val count = snapshot.count(value).toString()
            logger.send(count)
            return count
        }

        suspend fun set(key: String, value: String): String {
            snapshot.set(key, value)
            return ""
        }

        suspend fun delete(key: String): String {
            snapshot.set(key, null)
            return ""
        }

        fun commit(): String {
            successful = true
            return ""
        }

        fun rollback(): String {
            successful = false
            return ""
        }
    }
}