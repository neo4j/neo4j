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

import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.LegacyIndexApplier.ProviderLookup;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.CacheInvalidationTransactionApplier;
import org.neo4j.kernel.impl.transaction.command.HighIdTransactionApplier;
import org.neo4j.kernel.impl.transaction.command.IndexTransactionApplier;
import org.neo4j.kernel.impl.transaction.command.NeoCommandHandler;
import org.neo4j.kernel.impl.transaction.command.NeoStoreTransactionApplier;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.impl.util.function.Optional;

/**
 * Holistic application of {@link TransactionRepresentation transactions} onto the store. Includes application
 * for the graph store, schema indexes and legacy indexes. It's expected that there's only one instance
 * of this type for any given database.
 */
public class TransactionRepresentationStoreApplier
{
    private final NeoStore neoStore;
    private final IndexingService indexingService;
    private final CacheAccessBackDoor cacheAccess;
    private final LockService lockService;
    private final LabelScanStore labelScanStore;
    private final IndexConfigStore indexConfigStore;
    private final ProviderLookup legacyIndexProviderLookup;
    private final IdOrderingQueue legacyIndexTransactionOrdering;

    public TransactionRepresentationStoreApplier(
            IndexingService indexingService, LabelScanStore labelScanStore, NeoStore neoStore,
            CacheAccessBackDoor cacheAccess, LockService lockService, ProviderLookup legacyIndexProviderLookup,
            IndexConfigStore indexConfigStore, IdOrderingQueue legacyIndexTransactionOrdering )
    {
        this.indexingService = indexingService;
        this.labelScanStore = labelScanStore;
        this.neoStore = neoStore;
        this.cacheAccess = cacheAccess;
        this.lockService = lockService;
        this.legacyIndexProviderLookup = legacyIndexProviderLookup;
        this.indexConfigStore = indexConfigStore;
        this.legacyIndexTransactionOrdering = legacyIndexTransactionOrdering;
    }

    public void apply( TransactionRepresentation representation, ValidatedIndexUpdates indexUpdates, LockGroup locks,
                       long transactionId, TransactionApplicationMode mode )
            throws IOException
    {
        // Graph store application. The order of the decorated store appliers is irrelevant
        NeoCommandHandler storeApplier = new NeoStoreTransactionApplier(
                neoStore, cacheAccess, lockService, locks, transactionId );
        if ( mode.needsIdTracking() )
        {
            storeApplier = new HighIdTransactionApplier( storeApplier, neoStore );
        }
        if ( mode.needsCacheInvalidationOnUpdates() )
        {
            storeApplier = new CacheInvalidationTransactionApplier( storeApplier, neoStore, cacheAccess );
        }

        // Schema index application
        IndexTransactionApplier indexApplier = new IndexTransactionApplier( indexingService, indexUpdates,
                labelScanStore );

        // Legacy index application
        LegacyIndexApplier legacyIndexApplier = new LegacyIndexApplier( indexConfigStore,
                legacyIndexProviderLookup, legacyIndexTransactionOrdering, transactionId, mode );

        // Counts store application
        NeoCommandHandler countsStoreApplier = getCountsStoreApplier( transactionId, mode );

        // Perform the application
        try ( CommandApplierFacade applier = new CommandApplierFacade(
                storeApplier, indexApplier, legacyIndexApplier, countsStoreApplier ) )
        {
            representation.accept( applier );
        }
    }

    private NeoCommandHandler getCountsStoreApplier( long transactionId, TransactionApplicationMode mode )
    {
        Optional<NeoCommandHandler> handlerOption = neoStore.getCounts().apply( transactionId )
                                                            .map( CountsStoreApplier.FACTORY );
        if ( mode == TransactionApplicationMode.RECOVERY )
        {
            handlerOption = handlerOption.or( NeoCommandHandler.EMPTY );
        }
        return handlerOption.get();
    }

    public TransactionRepresentationStoreApplier withLegacyIndexTransactionOrdering(
            IdOrderingQueue legacyIndexTransactionOrdering )
    {
        return new TransactionRepresentationStoreApplier( indexingService, labelScanStore, neoStore, cacheAccess,
                                                          lockService, legacyIndexProviderLookup, indexConfigStore,
                                                          legacyIndexTransactionOrdering );
    }
}
