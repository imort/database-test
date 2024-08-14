package io.github.imort.database.command

import io.github.imort.database.command.Command.Begin
import io.github.imort.database.command.Command.GeneralCommand.Commit
import io.github.imort.database.command.Command.GeneralCommand.Count
import io.github.imort.database.command.Command.GeneralCommand.Delete
import io.github.imort.database.command.Command.GeneralCommand.Get
import io.github.imort.database.command.Command.GeneralCommand.Rollback
import io.github.imort.database.command.Command.GeneralCommand.Set

object CommandParser {
    private val commandFactories = mapOf<String, (List<String>) -> Command>(
        "get" to ::get,
        "count" to ::count,
        "set" to ::set,
        "delete" to ::delete,
        "begin" to ::begin,
        "commit" to ::commit,
        "rollback" to ::rollback,
    )

    fun parse(input: String): Command {
        require(input.isNotBlank()) { "Blank input" }
        val parts = input.split(" ").filter(String::isNotBlank)
        val commandName = parts.first().lowercase()
        val commandArgs = parts.drop(1)
        require(commandName in commandFactories.keys) { "Unknown command: $commandName" }
        return commandFactories[commandName]?.invoke(commandArgs) ?: error("Unknown command: $commandName")
    }

    private fun get(args: List<String>): Get {
        require(args.isNotEmpty()) { "Get command requires 1 argument" }
        return Get(args[0])
    }

    private fun count(args: List<String>): Count {
        require(args.isNotEmpty()) { "Count command requires 1 argument" }
        return Count(args[0])
    }

    private fun set(args: List<String>): Set {
        require(args.size >= 2) { "Set command requires 2 arguments" }
        return Set(args[0], args[1])
    }

    private fun delete(args: List<String>): Delete {
        require(args.isNotEmpty()) { "Delete command requires 1 argument" }
        return Delete(args[0])
    }

    @Suppress("UNUSED_PARAMETER")
    private fun begin(args: List<String>): Begin {
        return Begin
    }

    @Suppress("UNUSED_PARAMETER")
    private fun commit(args: List<String>): Commit {
        return Commit
    }

    @Suppress("UNUSED_PARAMETER")
    private fun rollback(args: List<String>): Rollback {
        return Rollback
    }
}