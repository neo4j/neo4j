/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
    private KernelHealth health;

    public BatchingTransactionRepresentationStoreApplier( IndexingService indexingService,
            LabelScanStore labelScanStore, NeoStores neoStore, CacheAccessBackDoor cacheAccess,
            LockService lockService, LegacyIndexApplierLookup legacyIndexProviderLookup,
            IndexConfigStore indexConfigStore, KernelHealth kernelHealth, IdOrderingQueue legacyIndexTransactionOrdering )
    {
        this( indexingService, new RecoveryLabelScanWriterProvider( labelScanStore, 1000 ),
                neoStore, cacheAccess, lockService,
                new RecoveryLegacyIndexApplierLookup( legacyIndexProviderLookup, 1000 ),
                indexConfigStore, kernelHealth, legacyIndexTransactionOrdering );
        this.health = kernelHealth;
    }

    private BatchingTransactionRepresentationStoreApplier(
            IndexingService indexingService,
            RecoveryLabelScanWriterProvider labelScanWriterProvider,
            NeoStores neoStore,
            CacheAccessBackDoor cacheAccess,
            LockService lockService,
            RecoveryLegacyIndexApplierLookup legacyIndexApplierLookup,
            IndexConfigStore indexConfigStore,
            KernelHealth kernelHealth,
            IdOrderingQueue legacyIndexTransactionOrdering )
    {
        super( indexingService, labelScanWriterProvider, neoStore, cacheAccess, lockService, legacyIndexApplierLookup,
                indexConfigStore, kernelHealth, legacyIndexTransactionOrdering );
        this.labelScanWriterProvider = labelScanWriterProvider;
        this.legacyIndexApplierLookup = legacyIndexApplierLookup;
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
