package io.github.imort.database

import dagger.Binds
import dagger.Module
import dagger.Provides
import dagger.hilt.InstallIn
import dagger.hilt.components.SingletonComponent
import dagger.hilt.migration.DisableInstallInCheck
import io.github.imort.database.store.StoreImpl
import io.github.imort.database.store.Store

@Module(includes = [DatabaseModule.Bindings::class])
@InstallIn(SingletonComponent::class)
internal object DatabaseModule {
    @Provides
    fun dispatchers(): DispatchersFactory = DispatchersFactory.DefaultDispatchersFactory

    @Module
    @DisableInstallInCheck
    interface Bindings {
        @Binds
        fun store(impl: StoreImpl): Store
    }
}