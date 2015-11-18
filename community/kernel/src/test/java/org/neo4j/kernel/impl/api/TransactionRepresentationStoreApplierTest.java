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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.ReentrantLockService;
import org.neo4j.kernel.impl.storageengine.StorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.TransactionApplicationMode.INTERNAL;
import static org.neo4j.kernel.impl.util.function.Optionals.some;

public class TransactionRepresentationStoreApplierTest
{
    private final IndexingService indexService = mock( IndexingService.class );
    @SuppressWarnings( "unchecked" )
    private final Supplier<LabelScanWriter> labelScanStore = mock( Supplier.class );
    private final NeoStores neoStores = mock( NeoStores.class );
    private final CacheAccessBackDoor cacheAccess = mock( CacheAccessBackDoor.class );
    private final LockService lockService = new ReentrantLockService();
    private final LegacyIndexApplierLookup legacyIndexProviderLookup =
            mock( LegacyIndexApplierLookup.class );
    private final IndexConfigStore indexConfigStore = mock( IndexConfigStore.class );
    private final IdOrderingQueue queue = mock( IdOrderingQueue.class );
    private final KernelHealth kernelHealth = mock( KernelHealth.class );
    private final StorageEngine storageEngine = mock( StorageEngine.class );
    private final int transactionId = 12;

    @Before
    public void setUp()
    {
        final CountsTracker tracker = mock( CountsTracker.class );
        when( neoStores.getCounts() ).thenReturn( tracker );
        when( tracker.apply( anyLong() ) ).thenReturn( some( mock( CountsAccessor.Updater.class ) ) );
        when( storageEngine.indexingService() ).thenReturn( indexService );
        when( storageEngine.cacheAccess() ).thenReturn( cacheAccess );
        when( storageEngine.legacyIndexApplierLookup() ).thenReturn( legacyIndexProviderLookup );
        when( storageEngine.neoStores() ).thenReturn( neoStores );
        when( storageEngine.kernelHealth() ).thenReturn( kernelHealth );
    }

    @Test
    public void transactionRepresentationShouldAcceptApplierVisitor() throws IOException
    {
        TransactionRepresentationStoreApplier applier = createStoreApplier();

        TransactionRepresentation transaction = mock( TransactionRepresentation.class );

        try ( LockGroup locks = new LockGroup() )
        {
            applier.apply( transaction, ValidatedIndexUpdates.NONE, locks, transactionId, INTERNAL );
        }

        verify( transaction, times( 1 ) ).accept( Matchers.<Visitor<Command,IOException>>any() );
    }

    private TransactionRepresentationStoreApplier createStoreApplier()
    {
        return new TransactionRepresentationStoreApplier( labelScanStore,
                lockService, indexConfigStore, queue, legacyIndexProviderLookup, neoStores, cacheAccess,
                indexService, kernelHealth );
    }

    @Test
    public void shouldUpdateIdGeneratorsOnExternalCommit() throws IOException
    {
        // GIVEN
        NodeStore nodeStore = mock( NodeStore.class );
        when( neoStores.getNodeStore() ).thenReturn( nodeStore );
        TransactionRepresentationStoreApplier applier = createStoreApplier();
        long nodeId = 5L;
        TransactionRepresentation transaction = createNodeTransaction( nodeId );

        // WHEN
        try ( LockGroup locks = new LockGroup() )
        {
            applier.apply( transaction, ValidatedIndexUpdates.NONE, locks, transactionId, TransactionApplicationMode
                    .EXTERNAL );
        }
        verify( nodeStore, times( 1 ) ).setHighestPossibleIdInUse( nodeId );
    }

    @Test
    public void shouldNotifyIdQueueWhenAppliedToLegacyIndexes() throws Exception
    {
        // GIVEN
        TransactionRepresentationStoreApplier applier = createStoreApplier();
        TransactionRepresentation transaction = new PhysicalTransactionRepresentation( indexTransaction() );

        // WHEN
        try ( LockGroup locks = new LockGroup() )
        {
            applier.apply( transaction, ValidatedIndexUpdates.NONE, locks, transactionId, INTERNAL );
        }

        // THEN
        verify( queue ).removeChecked( transactionId );
    }

    @Test
    public void shouldPanicOnIOExceptions() throws Exception
    {
        // GIVEN
        TransactionRepresentationStoreApplier applier = createStoreApplier();
        TransactionRepresentation transaction = mock( TransactionRepresentation.class );
        IOException ioex = new IOException();
        //noinspection unchecked
        doThrow( ioex ).when( transaction ).accept( any( Visitor.class ) );

        // WHEN
        try ( LockGroup locks = new LockGroup() )
        {
            applier.apply( transaction, ValidatedIndexUpdates.NONE, locks, transactionId, INTERNAL );
            fail( "should have thrown" );
        }
        catch ( IOException ex )
        {
            // THEN
            assertSame( ioex, ex );
            verify( kernelHealth, times( 1 ) ).panic( ioex );
        }
    }

    private Collection<Command> indexTransaction()
    {
        IndexDefineCommand definitions = new IndexDefineCommand();
        definitions.init(
                MapUtil.<String,Integer>genericMap( "one", 1 ),
                MapUtil.<String,Integer>genericMap( "two", 2 ) );
        return Collections.<Command>singletonList( definitions );
    }

    private TransactionRepresentation createNodeTransaction( long nodeId )
    {
        return new PhysicalTransactionRepresentation( Collections.singletonList( createNodeCommand( nodeId ) ) );
    }

    private Command createNodeCommand( long nodeId )
    {
        NodeCommand command = new NodeCommand();
        NodeRecord after = new NodeRecord( nodeId );
        after.setInUse( true );
        command.init( new NodeRecord( nodeId ), after );
        return command;
    }
}
