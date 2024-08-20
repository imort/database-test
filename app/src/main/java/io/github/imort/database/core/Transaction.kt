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
import java.util.UUID

class Transaction(
    val logger: Channel<String>,
    val snapshot: Snapshot<String>,
) {
    val id = UUID.randomUUID().toString()
    var successful: Boolean = false
    val scope = TransactionScope()

    inner class TransactionScope {
        fun perform(command: Command.GeneralCommand): String = when (command) {
            is Get -> get(command.key)
            is Count -> count(command.value)
            is Set -> set(command.key, command.value)
            is Delete -> delete(command.key)
            is Commit -> commit()
            is Rollback -> rollback()
        }

        fun get(key: String): String {
            val value = snapshot.get(key)
            val message = value ?: "Key $key not set"
            logger.trySend(message)
            return message
        }

        fun count(value: String): String {
            val count = snapshot.count(value).toString()
            logger.trySend(count)
            return count
        }

        fun set(key: String, value: String): String {
            snapshot.set(key, value)
            return ""
        }

        fun delete(key: String): String {
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