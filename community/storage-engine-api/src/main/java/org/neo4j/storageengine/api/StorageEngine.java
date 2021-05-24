/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.api;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.neo4j.counts.CountsAccessor;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.InjectedNLIUpgradeCallback;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.lock.LockGroup;
import org.neo4j.lock.LockService;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.logging.Log;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

/**
 * A StorageEngine provides the functionality to durably store data, and read it back.
 */
public interface StorageEngine extends Lifecycle
{
    /**
     * @return a new {@link CommandCreationContext} meant to be kept for multiple calls to
     * {@link #createCommands(Collection, ReadableTransactionState, StorageReader, CommandCreationContext, ResourceLocker, LockTracer, long,
     * TxStateVisitor.Decorator, CursorContext, MemoryTracker)}.
     * Must be {@link CommandCreationContext#close() closed} after used, before being discarded.
     */
    CommandCreationContext newCommandCreationContext( MemoryTracker memoryTracker );

    /**
     * Adds an {@link IndexUpdateListener} which will receive streams of index updates from changes that gets
     * {@link #apply(CommandsToApply, TransactionApplicationMode) applied} to this storage engine.
     * @param indexUpdateListener {@link IndexUpdateListener} to add.
     */
    void addIndexUpdateListener( IndexUpdateListener indexUpdateListener );

    /**
     * Generates a list of {@link StorageCommand commands} representing the changes in the given transaction state
     * ({@code state}.
     * The returned commands can be used to form {@link CommandsToApply} batches, which can be applied to this
     * storage using {@link #apply(CommandsToApply, TransactionApplicationMode)}.
     * The reason this is separated like this is that the generated commands can be used for other things
     * than applying to storage, f.ex replicating to another storage engine.
     * @param target {@link Collection} to put {@link StorageCommand commands} into.
     * @param state {@link ReadableTransactionState} representing logical store changes to generate commands for.
     * @param storageReader {@link StorageReader} to use for reading store state during creation of commands.
     * @param creationContext {@link CommandCreationContext} to use for do contextualized command creation e.g. id allocation.
     * @param locks {@link ResourceLocker} can grab additional locks.
     * This locks client still have the potential to acquire more locks at this point.
     * TODO we should try to get rid of this locking mechanism during creation of commands
     * The reason it's needed is that some relationship changes in the record storage engine
     * needs to lock prev/next relationships and these changes happens when creating commands
     * The EntityLocker interface is a subset of Locks.Client interface, just to fit in while it's here.
     * @param lockTracer traces additional locks acquired while creating commands.
     * @param lastTransactionIdWhenStarted transaction id which was seen as last committed when this
     * transaction started, i.e. before any changes were made and before any data was read.
     * @param additionalTxStateVisitor any additional tx state visitor decoration.
     * @param cursorContext underlying page cursor context
     * @param memoryTracker to report allocations to
     * @throws KernelException on known errors while creating commands.
     */
    void createCommands(
            Collection<StorageCommand> target,
            ReadableTransactionState state,
            StorageReader storageReader,
            CommandCreationContext creationContext,
            ResourceLocker locks,
            LockTracer lockTracer,
            long lastTransactionIdWhenStarted,
            TxStateVisitor.Decorator additionalTxStateVisitor,
            CursorContext cursorContext,
            MemoryTracker memoryTracker )
            throws KernelException;

    /**
     * Generates a list of {@link StorageCommand commands} representing the upgrade of the {@link KernelVersion} for this
     * store to the desired version.
     * @param versionToUpgradeTo the {@link KernelVersion} that the returned commands will represent an upgrade to.
     * @param injectedNLIUpgradeCallback callback for token indexes upgrade commands
     * @return commands for making an upgrade to the desired {@link KernelVersion}.
     */
    List<StorageCommand> createUpgradeCommands( KernelVersion versionToUpgradeTo, InjectedNLIUpgradeCallback injectedNLIUpgradeCallback );

    /**
     * Claims exclusive locks for some records whilst performing recovery.
     * Note: only used when {@code unsupported.dbms.recovery.enable_parallelism=true}
     *
     * @param commands whose records may need locking for safe parallel recovery.
     * @param lockService used to acquire locks on records during recovery
     * @param lockGroup collection of acquired locks
     * @param mode used in this case to distinguish between RECOVERY and REVERSE_RECOVERY
     */
    void lockRecoveryCommands( CommandStream commands, LockService lockService, LockGroup lockGroup, TransactionApplicationMode mode );

    /**
     * Apply a batch of groups of commands to this storage.
     *
     * @param batch batch of groups of commands to apply to storage.
     * @param mode {@link TransactionApplicationMode} when applying.
     * @throws Exception if an error occurs during application.
     */
    void apply( CommandsToApply batch, TransactionApplicationMode mode ) throws Exception;

    /**
     * Flushes and forces all changes down to underlying storage. This is a blocking call and when it returns
     * all changes applied to this storage engine will be durable.
     *
     * @param cursorContext underlying page cursor context
     * @throws IOException on I/O error.
     */
    void flushAndForce( CursorContext cursorContext ) throws IOException;

    /**
     * Dump diagnostics about the storage.
     *
     * @param errorLog to which to log error messages.
     * @param diagnosticsLog to which to log diagnostics messages.
     */
    void dumpDiagnostics( Log errorLog, DiagnosticsLogger diagnosticsLog );

    /**
     * Force close all opened resources. This may be called during startup if there's a failure
     * during recovery or similar.
     */
    void forceClose();

    /**
     * Lists storage files into one of the two provided collections.
     * @param atomic will contain files that must be copied under a lock where no checkpoint can happen concurrently.
     * @param replayable will contain files not sensitive to the checkpoint constraint of those in the {@code atomic} collection.
     */
    void listStorageFiles( Collection<StoreFileMetadata> atomic, Collection<StoreFileMetadata> replayable );

    /**
     * @return the {@link StoreId} of the underlying store.
     */
    StoreId getStoreId();

    /**
     * The life cycle that is used for initialising the token holders, and filling the schema cache.
     */
    Lifecycle schemaAndTokensLifecycle();

    /**
     * @return a {@link MetadataProvider}, provides access to underlying storage metadata information.
     */
    MetadataProvider metadataProvider();

    CountsAccessor countsAccessor();

    /**
     * Creates a new {@link StorageReader} for reading committed data from the underlying storage.
     * The returned instance is intended to be used by one transaction at a time, although can and should be reused
     * for multiple transactions.
     *
     * @return an interface for accessing data in the storage.
     */
    StorageReader newReader();
}
