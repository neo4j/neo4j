/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.api;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.neo4j.configuration.Config;
import org.neo4j.counts.CountsStore;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.io.pagecache.OutOfDiskSpaceException;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.lock.LockGroup;
import org.neo4j.lock.LockService;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.logging.InternalLog;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.enrichment.Enrichment;
import org.neo4j.storageengine.api.enrichment.EnrichmentCommand;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TransactionStateBehaviour;
import org.neo4j.storageengine.api.txstate.TxStateVisitor.Decorator;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidatorFactory;

/**
 * A StorageEngine provides the functionality to durably store data, and read it back.
 */
public interface StorageEngine extends ReadableStorageEngine, Lifecycle {
    /**
     * @return the name of this storage engine, which will be used in e.g. storage engine selection and settings.
     */
    String name();

    /**
     * @return the unique id of this storage engine which can be used in e.g. storage engine selection
     */
    byte id();

    /**
     * @return a new {@link CommandCreationContext} meant to be kept for multiple calls to
     * {@link #createCommands(ReadableTransactionState, StorageReader, CommandCreationContext, LockTracer, Decorator, CursorContext, StoreCursors, MemoryTracker)}.
     * Must be {@link CommandCreationContext#close() closed} after used, before being discarded.
     */
    CommandCreationContext newCommandCreationContext(boolean multiVersioned);

    /**
     * Create multi versioned stores transaction validator factory. Validator factory produces noop validators in all other engines.
     */
    TransactionValidatorFactory createTransactionValidatorFactory(Config config);

    StorageLocks createStorageLocks(ResourceLocker locker);

    /**
     * Adds an {@link IndexUpdateListener} which will receive streams of index updates from changes that gets
     * {@link #apply(CommandBatchToApply, TransactionApplicationMode) applied} to this storage engine.
     * @param indexUpdateListener {@link IndexUpdateListener} to add.
     */
    void addIndexUpdateListener(IndexUpdateListener indexUpdateListener);

    /**
     * Generates a list of {@link StorageCommand commands} representing the changes in the given transaction state
     * ({@code state}.
     * The returned commands can be used to form {@link CommandBatchToApply} batches, which can be applied to this
     * storage using {@link #apply(CommandBatchToApply, TransactionApplicationMode)}.
     * The reason this is separated like this is that the generated commands can be used for other things
     * than applying to storage, f.ex replicating to another storage engine.
     * @param state {@link ReadableTransactionState} representing logical store changes to generate commands for.
     * @param storageReader {@link StorageReader} to use for reading store state during creation of commands.
     * @param creationContext {@link CommandCreationContext} to use for do contextualized command creation e.g. id allocation.
     * @param lockTracer traces additional locks acquired while creating commands.
     * @param additionalTxStateVisitor any additional tx state visitor decoration.
     * @param cursorContext underlying page cursor context
     * @param memoryTracker to report allocations to
     * @throws KernelException on known errors while creating commands.
     */
    List<StorageCommand> createCommands(
            ReadableTransactionState state,
            StorageReader storageReader,
            CommandCreationContext creationContext,
            LockTracer lockTracer,
            Decorator additionalTxStateVisitor,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker)
            throws KernelException;

    /**
     * The storage-engine specific mechanism for creating {@link EnrichmentCommand}s.
     * NB The created command will have no interactions with the stores.
     * @param kernelVersion the transaction's {@link KernelVersion}
     * @param enrichment the enrichment data to wrap
     * @return the storage-engine specific {@link EnrichmentCommand}
     */
    EnrichmentCommand createEnrichmentCommand(KernelVersion kernelVersion, Enrichment enrichment);

    /**
     * Claims exclusive locks for some records whilst performing recovery.
     * Note: only used when {@code internal.dbms.recovery.enable_parallelism=true}
     *
     * @param commands whose records may need locking for safe parallel recovery.
     * @param lockService used to acquire locks on records during recovery
     * @param lockGroup collection of acquired locks
     * @param mode used in this case to distinguish between RECOVERY and REVERSE_RECOVERY
     */
    void lockRecoveryCommands(
            CommandStream commands, LockService lockService, LockGroup lockGroup, TransactionApplicationMode mode)
            throws IOException;

    /**
     * Apply a batch of groups of commands to this storage.
     *
     * @param batch batch of groups of commands to apply to storage.
     * @param mode {@link TransactionApplicationMode} when applying.
     * @throws Exception if an error occurs during application.
     */
    void apply(CommandBatchToApply batch, TransactionApplicationMode mode) throws Exception;

    /**
     * Called for a transaction to release any storage engine resources on close
     * @param txState transaction state of the transaction.
     * @param cursorContext transactional cursor context.
     * @param rolledBack true if transaction was rolled back
     */
    void release(
            ReadableTransactionState txState,
            CursorContext cursorContext,
            CommandCreationContext commandCreationContext,
            boolean rolledBack);

    /**
     * Checkpoints underlying storage. Leaves no guarantee that files are flushed to persistable storage afterwards
     * @param flushEvent flush event from checkpoint
     * @param cursorContext underlying page cursor context
     * @throws IOException on I/O error.
     */
    void checkpoint(DatabaseFlushEvent flushEvent, CursorContext cursorContext) throws IOException;

    /**
     * Dump diagnostics about the storage.
     *
     * @param errorLog to which to log error messages.
     * @param diagnosticsLog to which to log diagnostics messages.
     */
    void dumpDiagnostics(InternalLog errorLog, DiagnosticsLogger diagnosticsLog);

    /**
     * Close all opened resources. This may be called during startup if there's a failure
     * during recovery or similar. That can happen outside of the owning lifecycle if any.
     */
    @Override
    void shutdown();

    /**
     * Lists storage files into one of the two provided collections.
     * @param atomic will contain files that must be copied under a lock where no checkpoint can happen concurrently.
     * @param replayable will contain files not sensitive to the checkpoint constraint of those in the {@code atomic} collection.
     */
    void listStorageFiles(Collection<StoreFileMetadata> atomic, Collection<StoreFileMetadata> replayable);

    /**
     * Add id files into the provided collection.
     */
    void listIdFiles(Collection<StoreFileMetadata> target);

    StoreId retrieveStoreId();

    /**
     * The life cycle that is used for initialising the token holders, and filling the schema cache.
     */
    Lifecycle schemaAndTokensLifecycle();

    /**
     * @return a {@link MetadataProvider}, provides access to underlying storage metadata information.
     */
    MetadataProvider metadataProvider();

    CountsStore countsAccessor();

    /**
     * @return a {@link StoreEntityCounters}, providing access to underlying store entity counters.
     */
    StoreEntityCounters storeEntityCounters();

    /**
     * @return a {@link InternalErrorTracer}, providing trace information on internal errors.
     */
    InternalErrorTracer internalErrorTracer();

    /**
     * @return specific behaviour of transaction state that is optimal for this storage engine.
     */
    default TransactionStateBehaviour transactionStateBehaviour() {
        return TransactionStateBehaviour.DEFAULT_BEHAVIOUR;
    }

    /**
     * Preallocate disk space for a batch of groups of commands to this storage.
     *
     * @param batch batch of groups of commands to preallocate disk space for.
     * @param mode {@link TransactionApplicationMode} that can affect if allocation needs to happen.
     * @throws OutOfDiskSpaceException if preallocation failed due to lack of disk space.
     * @throws IOException if preallocation failed for a different reason.
     */
    void preAllocateStoreFilesForCommands(CommandBatchToApply batch, TransactionApplicationMode mode)
            throws OutOfDiskSpaceException, IOException;

    /**
     * Conservatively estimate how much reserved space is available for (re)use.
     * @return available reserved space estimate in bytes
     * @throws IOException on error reading from store.
     */
    default long estimateAvailableReservedSpace() {
        return 0L;
    }
}
