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
package org.neo4j.kernel.impl.api;

import java.io.IOException;

import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.RecoveryLabelScanWriterProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.util.IdOrderingQueue;

/**
 * {@link TransactionRepresentationStoreApplier} that builds services made for batching transactions.
 * Transaction data can be cached and applied as one batch when a threshold is reached, so ensuring that transaction
 * data is actually written will have to be done by calling {@link #closeBatch()}.
 */
public class BatchingTransactionRepresentationStoreApplier extends TransactionRepresentationStoreApplier
{
    private final RecoveryLabelScanWriterProvider labelScanWriterProvider;
    private final RecoveryLegacyIndexApplierLookup legacyIndexApplierLookup;
    private final KernelHealth health;
    private final IndexingService indexingService;

    public BatchingTransactionRepresentationStoreApplier(
            LockService lockService,
            IndexConfigStore indexConfigStore,
            IdOrderingQueue legacyIndexTransactionOrdering,
            LabelScanStore labelScanStore,
            LegacyIndexApplierLookup legacyIndexApplierLookup,
            NeoStores neoStores,
            CacheAccessBackDoor cacheAccess,
            IndexingService indexingService,
            KernelHealth kernelHealth )
    {
        this( new RecoveryLabelScanWriterProvider( labelScanStore, 1000 ),
                lockService,
                new RecoveryLegacyIndexApplierLookup( legacyIndexApplierLookup, 1000 ),
                indexConfigStore, legacyIndexTransactionOrdering, neoStores, cacheAccess,
                indexingService, kernelHealth );
    }

    private BatchingTransactionRepresentationStoreApplier(
            RecoveryLabelScanWriterProvider labelScanWriterProvider,
            LockService lockService,
            RecoveryLegacyIndexApplierLookup legacyIndexApplierLookup,
            IndexConfigStore indexConfigStore,
            IdOrderingQueue legacyIndexTransactionOrdering,
            NeoStores neoStores,
            CacheAccessBackDoor cacheAccess,
            IndexingService indexingService,
            KernelHealth kernelHealth )
    {
        super( labelScanWriterProvider, lockService,
                indexConfigStore, legacyIndexTransactionOrdering, legacyIndexApplierLookup, neoStores,
                cacheAccess, indexingService, kernelHealth );
        this.labelScanWriterProvider = labelScanWriterProvider;
        this.legacyIndexApplierLookup = legacyIndexApplierLookup;
        this.health = kernelHealth;
        this.indexingService = indexingService;
    }

    public void closeBatch() throws IOException
    {
        try
        {
            labelScanWriterProvider.close();
            legacyIndexApplierLookup.close();
            indexingService.flushAll();
        }
        catch ( Throwable ex )
        {
            health.panic( ex );
            throw ex;
        }
    }
}
