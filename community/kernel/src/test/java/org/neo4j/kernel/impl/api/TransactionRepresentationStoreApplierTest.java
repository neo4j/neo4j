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
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.mockito.Matchers;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier.HighIdTrackerFactory;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.HighIdTracker;
import org.neo4j.kernel.impl.transaction.xaframework.IdOrderingQueue;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier.DEFAULT_HIGH_ID_TRACKING;
import static org.neo4j.kernel.impl.transaction.xaframework.IdOrderingQueue.BYPASS;

public class TransactionRepresentationStoreApplierTest
{
    private final IndexingService indexService = mock( IndexingService.class );
    private final LabelScanStore labelScanStore = mock( LabelScanStore.class );
    private final NeoStore neoStore = mock( NeoStore.class );
    private final CacheAccessBackDoor cacheAccess = mock( CacheAccessBackDoor.class );
    private final LockService lockService = mock( LockService.class );
    private final LegacyIndexApplier.ProviderLookup legacyIndexProviderLookup =
            mock( LegacyIndexApplier.ProviderLookup.class );
    private final IndexConfigStore indexConfigStore = mock( IndexConfigStore.class );
    private final int transactionId = 12;

    @Test
    public void transactionRepresentationShouldAcceptApplierVisitor() throws IOException
    {
        TransactionRepresentationStoreApplier applier = new TransactionRepresentationStoreApplier( indexService,
                labelScanStore, neoStore, cacheAccess, lockService, legacyIndexProviderLookup, indexConfigStore,
                DEFAULT_HIGH_ID_TRACKING, BYPASS );

        TransactionRepresentation transaction = mock( TransactionRepresentation.class );

        try ( LockGroup locks = new LockGroup() )
        {
            applier.apply( transaction, locks, transactionId, false );
        }

        verify( transaction, times( 1 ) ).accept( Matchers.<Visitor<Command, IOException>>any() );
    }

    @Test
    public void shouldUpdateIdGeneratorsWhenOnRecovery() throws IOException
    {
        HighIdTracker tracker = mock( HighIdTracker.class );
        HighIdTrackerFactory highIdTrackerFactory = mock( HighIdTrackerFactory.class );
        when( highIdTrackerFactory.create( true ) ).thenReturn( tracker );
        TransactionRepresentationStoreApplier applier = new TransactionRepresentationStoreApplier( indexService,
                labelScanStore, neoStore, cacheAccess, lockService, legacyIndexProviderLookup, indexConfigStore,
                highIdTrackerFactory, BYPASS );

        TransactionRepresentation transaction = mock( TransactionRepresentation.class );

        try ( LockGroup locks = new LockGroup() )
        {
            applier.apply( transaction, locks, transactionId, true );
        }

        verify( transaction, times( 1 ) ).accept( Matchers.<Visitor<Command, IOException>>any() );
        verify( tracker, times( 1 ) ).apply();
    }

    @Test
    public void shouldNotifyIdQueueWhenAppliedToLegacyIndexes() throws Exception
    {
        // GIVEN
        IdOrderingQueue queue = mock( IdOrderingQueue.class );
        TransactionRepresentationStoreApplier applier = new TransactionRepresentationStoreApplier( indexService,
                labelScanStore, neoStore, cacheAccess, lockService, legacyIndexProviderLookup, indexConfigStore,
                DEFAULT_HIGH_ID_TRACKING, queue );
        TransactionRepresentation transaction = new PhysicalTransactionRepresentation( indexTransaction() );

        // WHEN
        try ( LockGroup locks = new LockGroup() )
        {
            applier.apply( transaction, locks, transactionId, false );
        }

        // THEN
        verify( queue ).removeChecked( transactionId );
    }

    private Collection<Command> indexTransaction()
    {
        IndexDefineCommand definitions = new IndexDefineCommand();
        definitions.init(
                MapUtil.<String,Byte>genericMap( "one", (byte)1 ),
                MapUtil.<String,Byte>genericMap( "two", (byte)2 ) );
        return Arrays.<Command>asList( definitions );
    }
}
