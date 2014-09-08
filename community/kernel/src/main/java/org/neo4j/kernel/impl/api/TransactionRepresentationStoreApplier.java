/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.PropertyLoader;
import org.neo4j.kernel.impl.nioneo.xa.command.HighIdTracker;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoCommandHandler;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoTransactionIndexApplier;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoTransactionStoreApplier;
import org.neo4j.kernel.impl.nioneo.xa.command.RecoveredHighIdTracker;
import org.neo4j.kernel.impl.transaction.xaframework.IdOrderingQueue;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;

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
    private final PropertyLoader propertyLoader;
    private final HighIdTrackerFactory highIdTrackerFactory;
    private final IdOrderingQueue legacyIndexTransactionOrdering;

    public TransactionRepresentationStoreApplier(
            IndexingService indexingService, LabelScanStore labelScanStore, NeoStore neoStore,
            CacheAccessBackDoor cacheAccess, LockService lockService, ProviderLookup legacyIndexProviderLookup,
            IndexConfigStore indexConfigStore, HighIdTrackerFactory highIdTrackerFactory,
            IdOrderingQueue legacyIndexTransactionOrdering )
    {
        this.indexingService = indexingService;
        this.labelScanStore = labelScanStore;
        this.neoStore = neoStore;
        this.cacheAccess = cacheAccess;
        this.lockService = lockService;
        this.legacyIndexProviderLookup = legacyIndexProviderLookup;
        this.indexConfigStore = indexConfigStore;
        this.highIdTrackerFactory = highIdTrackerFactory;
        this.legacyIndexTransactionOrdering = legacyIndexTransactionOrdering;
        this.propertyLoader = new PropertyLoader( neoStore );
    }

    public void apply( TransactionRepresentation representation, LockGroup locks,
                       long transactionId, boolean applyRecovered )
            throws IOException
    {
        NeoCommandHandler storeApplier = new NeoTransactionStoreApplier(
                neoStore, indexingService, cacheAccess, lockService, locks, transactionId,
                highIdTrackerFactory, applyRecovered );
        NeoCommandHandler countStoreApplier = new CountStoreApplier( neoStore.getCountsStore() );
        NeoCommandHandler indexApplier = new NeoTransactionIndexApplier( indexingService,
                labelScanStore, neoStore.getNodeStore(), neoStore.getPropertyStore(), cacheAccess, propertyLoader );
        NeoCommandHandler legacyIndexApplier = new LegacyIndexApplier( indexConfigStore,
                legacyIndexProviderLookup, legacyIndexTransactionOrdering, transactionId, applyRecovered );

        try ( CommandApplierFacade applier = new CommandApplierFacade(
                storeApplier, countStoreApplier, indexApplier, legacyIndexApplier ) )
        {
            representation.accept( applier );
        }
    }
    
    public interface HighIdTrackerFactory
    {
        HighIdTracker create( boolean recovery );
    }
    
    public static final HighIdTrackerFactory DEFAULT_HIGH_ID_TRACKING = new HighIdTrackerFactory()
    {
        @Override
        public HighIdTracker create( boolean recovery )
        {
            return recovery ? new RecoveredHighIdTracker() : HighIdTracker.NO_TRACKING;
        }
    };
}
