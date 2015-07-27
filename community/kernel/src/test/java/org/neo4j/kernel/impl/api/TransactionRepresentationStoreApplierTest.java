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

import org.junit.Test;
import org.mockito.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.helpers.Provider;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.ReentrantLockService;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.util.function.Optionals.some;

public class TransactionRepresentationStoreApplierTest
{
    private final IndexingService indexService = mock( IndexingService.class );
    @SuppressWarnings( "unchecked" )
    private final Provider<LabelScanWriter> labelScanStore = mock( Provider.class );
    private final NeoStore neoStore = mock( NeoStore.class );
    private final CacheAccessBackDoor cacheAccess = mock( CacheAccessBackDoor.class );
    private final LockService lockService = new ReentrantLockService();
    private final LegacyIndexApplierLookup legacyIndexProviderLookup =
            mock( LegacyIndexApplierLookup.class );
    private final IndexConfigStore indexConfigStore = mock( IndexConfigStore.class );
    private final IdOrderingQueue queue = mock( IdOrderingQueue.class );
    private final int transactionId = 12;

    {
        final CountsTracker tracker = mock( CountsTracker.class );
        when( neoStore.getCounts() ).thenReturn( tracker );
        when( tracker.apply( anyLong() ) ).thenReturn( some( mock( CountsAccessor.Updater.class ) ) );
    }

    @Test
    public void transactionRepresentationShouldAcceptApplierVisitor() throws IOException
    {
        TransactionRepresentationStoreApplier applier = new TransactionRepresentationStoreApplier( indexService,
                labelScanStore, neoStore, cacheAccess, lockService, legacyIndexProviderLookup,
                indexConfigStore, queue );

        TransactionRepresentation transaction = mock( TransactionRepresentation.class );

        try ( LockGroup locks = new LockGroup() )
        {
            applier.apply( transaction, ValidatedIndexUpdates.NONE, locks, transactionId, TransactionApplicationMode.INTERNAL );
        }

        verify( transaction, times( 1 ) ).accept( Matchers.<Visitor<Command, IOException>>any() );
    }

    @Test
    public void shouldUpdateIdGeneratorsOnExternalCommit() throws IOException
    {
        // GIVEN
        NodeStore nodeStore = mock( NodeStore.class );
        when( neoStore.getNodeStore() ).thenReturn( nodeStore );
        TransactionRepresentationStoreApplier applier = new TransactionRepresentationStoreApplier( indexService,
                labelScanStore, neoStore, cacheAccess, lockService, legacyIndexProviderLookup, indexConfigStore, queue );
        long nodeId = 5L;
        TransactionRepresentation transaction = createNodeTransaction( nodeId );

        // WHEN
        try ( LockGroup locks = new LockGroup() )
        {
            applier.apply( transaction, ValidatedIndexUpdates.NONE, locks, transactionId, TransactionApplicationMode.EXTERNAL );
        }
        verify( nodeStore, times( 1 ) ).setHighestPossibleIdInUse( nodeId );
    }

    private TransactionRepresentation createNodeTransaction( long nodeId )
    {
        return new PhysicalTransactionRepresentation( Arrays.asList( createNodeCommand( nodeId ) ) );
    }

    private Command createNodeCommand( long nodeId )
    {
        NodeCommand command = new NodeCommand();
        NodeRecord after = new NodeRecord( nodeId );
        after.setInUse( true );
        command.init( new NodeRecord( nodeId ), after );
        return command;
    }

    @Test
    public void shouldNotifyIdQueueWhenAppliedToLegacyIndexes() throws Exception
    {
        // GIVEN
        IdOrderingQueue queue = mock( IdOrderingQueue.class );
        TransactionRepresentationStoreApplier applier = new TransactionRepresentationStoreApplier( indexService,
                labelScanStore, neoStore, cacheAccess, lockService, legacyIndexProviderLookup, indexConfigStore,
                queue );
        TransactionRepresentation transaction = new PhysicalTransactionRepresentation( indexTransaction() );

        // WHEN
        try ( LockGroup locks = new LockGroup() )
        {
            applier.apply( transaction, ValidatedIndexUpdates.NONE, locks, transactionId, TransactionApplicationMode.INTERNAL );
        }

        // THEN
        verify( queue ).removeChecked( transactionId );
    }

    private Collection<Command> indexTransaction()
    {
        IndexDefineCommand definitions = new IndexDefineCommand();
        definitions.init(
                MapUtil.<String,Integer>genericMap( "one", 1 ),
                MapUtil.<String,Integer>genericMap( "two", 2 ) );
        return Arrays.<Command>asList( definitions );
    }
}
