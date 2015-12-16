/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.storageengine;

import java.util.Collection;
import java.util.stream.Stream;

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.txstate.LegacyIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.LegacyIndexApplierLookup;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.store.ProcedureCache;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.Commitment;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.IntegrityValidator;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.unsafe.impl.batchimport.cache.NodeLabelsCache.Client;

/**
 * A StorageEngine provides the functionality to durably store data, and read it back.
 */
public interface StorageEngine
{
    /**
     * @return an interface for accessing data previously
     * {@link #apply(TransactionToApply, TransactionApplicationMode) applied} to this storage.
     */
    StoreReadLayer storeReadLayer();

    /**
     * Generates a list of {@link Command commands} representing the changes in the given transaction state
     * ({@code state} and {@code legacyIndexTransactionState}.
     * The returned commands can be used to form {@link TransactionRepresentation} and in extension
     * {@link TransactionToApply} batches, which can be applied to this storage using
     * {@link #apply(TransactionToApply, TransactionApplicationMode)}.
     * The reason this is separated like this is that the generated commands can be used for other things
     * than applying to storage, f.ex replicating to another storage engine.
     *
     * @param state {@link TransactionState} representing logical store changes to generate commands for.
     * @param legacyIndexTransactionState {@link LegacyIndexTransactionState} representing logical legacy index
     * changes to generate commands for.
     * @param locks {@link Client locks client} holding locks acquired during this transaction.
     * This locks client still have the potential to acquire more locks at this point.
     * @param operations {@link StatementOperationParts} additional data access needed when generating commands.
     * @param storeStatement {@link StoreStatement} additional data access needed when generating commands.
     * @param lastTransactionIdWhenStarted transaction id which was seen as last committed when this
     * transaction started, i.e. before any changes were made and before any data was read.
     * @return {@link Collection} of generated {@link Command commands}.
     * TODO Transitional (Collection), might be {@link Stream} or whatever.
     * @throws TransactionFailureException if command generation fails or some prerequisite of some command
     * didn't validate, for example if trying to delete a node that still has relationships.
     * @throws CreateConstraintFailureException if this transaction was set to create a constraint and that failed.
     * @throws ConstraintValidationKernelException if this transaction was set to create a constraint
     * and some data violates that constraint.
     */
    Collection<Command> createCommands(
            TransactionState state,
            LegacyIndexTransactionState legacyIndexTransactionState,
            Locks.Client locks,
            StatementOperationParts operations,
            StoreStatement storeStatement,
            long lastTransactionIdWhenStarted )
            throws TransactionFailureException, CreateConstraintFailureException, ConstraintValidationKernelException;

    /**
     * Apply a batch of transactions to this storage. Transactions are applied to storage after being committed,
     * i.e. appended to a log by {@link TransactionAppender}. Implementations should NOT
     * {@link Commitment#publishAsClosed() mark transactions as applied}, instead caller is expected to do that.
     * Caller is typically {@link TransactionCommitProcess}.
     *
     * @param batch batch of transactions to apply to storage.
     * @param mode {@link TransactionApplicationMode} when applying.
     * @throws Exception if an error occurs during application.
     */
    void apply( TransactionToApply batch, TransactionApplicationMode mode ) throws Exception;

    CommandReaderFactory commandReaderFactory();

    // ====================================================================
    // All these methods below are temporary while in the process of
    // creating this API, take little notice to them, as they will go away
    // ====================================================================
    @Deprecated
    TransactionIdStore transactionIdStore();

    @Deprecated
    LogVersionRepository logVersionRepository();

    @Deprecated
    ProcedureCache procedureCache();

    @Deprecated
    NeoStores neoStores();

    @Deprecated
    MetaDataStore metaDataStore();

    @Deprecated
    IndexingService indexingService();

    @Deprecated
    LabelScanStore labelScanStore();

    @Deprecated
    IntegrityValidator integrityValidator();

    @Deprecated
    SchemaIndexProviderMap schemaIndexProviderMap();

    @Deprecated
    CacheAccessBackDoor cacheAccess();

    @Deprecated
    LegacyIndexApplierLookup legacyIndexApplierLookup();

    @Deprecated
    IndexConfigStore indexConfigStore();

    @Deprecated
    IdOrderingQueue legacyIndexTransactionOrdering();

    @Deprecated
    void loadSchemaCache();
}
