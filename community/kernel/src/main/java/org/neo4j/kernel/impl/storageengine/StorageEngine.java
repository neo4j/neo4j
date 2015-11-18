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

import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.txstate.LegacyIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.LegacyIndexApplierLookup;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.store.ProcedureCache;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.IntegrityValidator;

/**
 * A StorageEngine provides the functionality to durably store data, and read it back.
 */
public interface StorageEngine
{
    StoreReadLayer storeReadLayer();

    // TODO Transitional (Collection), might be Stream or whatever
    Collection<Command> createCommands( TransactionState state, LegacyIndexTransactionState legacyIndexTransactionState,
            Locks.Client locks, long lastTransactionIdWhenStarted  )
            throws TransactionFailureException, CreateConstraintFailureException, ConstraintValidationKernelException;

    /**
     * @deprecated method to ease the transition a bit
     * We may want a <pre>void apply( CommandStream changes, long transactionId, ... );</pre> instead
     */
    @Deprecated
    TransactionRepresentationStoreApplier transactionApplier();

    @Deprecated
    TransactionRepresentationStoreApplier transactionApplierForRecovery();

//     Stream<Command> deserialise( SequenceOfBytes source );

    TransactionIdStore transactionIdStore();
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
    IndexUpdatesValidator indexUpdatesValidator();

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
    KernelHealth kernelHealth();

    void loadSchemaCache();
}
