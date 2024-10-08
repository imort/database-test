package io.github.imort.database.command

// Extendable set of commands
sealed interface Command {
    data object Begin : Command
    sealed interface GeneralCommand : Command {
        data class Get(val key: String) : GeneralCommand
        data class Count(val value: String) : GeneralCommand
        data class Set(val key: String, val value: String) : GeneralCommand
        data class Delete(val key: String) : GeneralCommand
        data object Commit : GeneralCommand
        data object Rollback : GeneralCommand
    }
}