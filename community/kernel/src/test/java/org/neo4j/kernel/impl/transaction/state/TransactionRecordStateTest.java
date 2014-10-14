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
package org.neo4j.kernel.impl.transaction.state;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CommandApplierFacade;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.NeoCommandHandler;
import org.neo4j.kernel.impl.transaction.command.NeoStoreTransactionApplier;
import org.neo4j.kernel.impl.transaction.log.CommandWriter;
import org.neo4j.kernel.impl.transaction.log.InMemoryVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.WritableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReaderFactory;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriterv1;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import static org.neo4j.helpers.collection.IteratorUtil.single;

public class TransactionRecordStateTest
{
    @Test
    public void shouldDeleteDynamicLabelsForDeletedNode() throws Exception
    {
        // GIVEN a store that has got a node with a dynamic label record
        NeoStore store = newNeoStore();
        NeoCommandHandler applier = new NeoStoreTransactionApplier( store, mock( CacheAccessBackDoor.class ),
                LockService.NO_LOCK_SERVICE, new LockGroup(), 1 );
        AtomicLong nodeId = new AtomicLong();
        AtomicLong dynamicLabelRecordId = new AtomicLong();
        apply( applier, transaction( nodeWithDynamicLabelRecord( store, nodeId, dynamicLabelRecordId ) ) );
        assertDynamicLabelRecordInUse( store, dynamicLabelRecordId.get(), true );

        // WHEN applying a transaction where the node is deleted
        apply( applier, transaction( deleteNode( store, nodeId.get() ) ) );

        // THEN the dynamic label record should also be deleted
        assertDynamicLabelRecordInUse( store, dynamicLabelRecordId.get(), false );
    }

    @Test
    public void shouldDeleteDynamicLabelsForDeletedNodeForRecoveredTransaction() throws Exception
    {
        // GIVEN a store that has got a node with a dynamic label record
        NeoStore store = newNeoStore();
        NeoCommandHandler applier = new NeoStoreTransactionApplier( store, mock( CacheAccessBackDoor.class ),
                LockService.NO_LOCK_SERVICE, new LockGroup(), 1 );
        AtomicLong nodeId = new AtomicLong();
        AtomicLong dynamicLabelRecordId = new AtomicLong();
        apply( applier, transaction( nodeWithDynamicLabelRecord( store, nodeId, dynamicLabelRecordId ) ) );
        assertDynamicLabelRecordInUse( store, dynamicLabelRecordId.get(), true );

        // WHEN applying a transaction, which has first round-tripped through a log (written then read)
        TransactionRepresentation transaction = transaction( deleteNode( store, nodeId.get() ) );
        InMemoryVersionableLogChannel channel = new InMemoryVersionableLogChannel();
        writeToChannel( transaction, channel );
        CommittedTransactionRepresentation recoveredTransaction = readFromChannel( channel );
        // and applying that recovered transaction
        apply( applier, recoveredTransaction.getTransactionRepresentation() );

        // THEN should have the dynamic label record should be deleted as well
        assertDynamicLabelRecordInUse( store, dynamicLabelRecordId.get(), false );
    }

    private CommittedTransactionRepresentation readFromChannel( ReadableVersionableLogChannel channel ) throws IOException
    {
        LogEntryReader<ReadableVersionableLogChannel> logEntryReader = new LogEntryReaderFactory().versionable();
        try ( PhysicalTransactionCursor<ReadableVersionableLogChannel> cursor =
                new PhysicalTransactionCursor<>( channel, logEntryReader ) )
        {
            assertTrue( cursor.next() );
            return cursor.get();
        }
    }

    private void writeToChannel( TransactionRepresentation transaction, WritableLogChannel channel )
            throws IOException
    {
        TransactionLogWriter writer = new TransactionLogWriter( new LogEntryWriterv1( channel,
                new CommandWriter( channel ) ) );
        writer.append( transaction, 2 );
    }

    private NeoStore newNeoStore()
    {
        File storeDir = new File( "dir" );
        fsr.get().mkdirs( storeDir );
        StoreFactory storeFactory = new StoreFactory( fsr.get(), storeDir,
                pageCacheRule.getPageCache( fsr.get(), new Config() ), StringLogger.DEV_NULL, new Monitors() );
        return cleanup.add( storeFactory.newNeoStore( true ) );
    }

    private TransactionRecordState nodeWithDynamicLabelRecord( NeoStore store,
            AtomicLong nodeId, AtomicLong dynamicLabelRecordId )
    {
        TransactionRecordStateContext context = context( store );
        TransactionRecordState recordState = recordState( store, context );

        nodeId.set( store.getNodeStore().nextId() );
        int[] labelIds = new int[20];
        for ( int i = 0; i < labelIds.length; i++ )
        {
            int labelId = (int) store.getLabelTokenStore().nextId();
            recordState.createLabelToken( "Label" + i, labelId );
            labelIds[i] = labelId;
        }
        recordState.nodeCreate( nodeId.get() );
        for ( int labelId : labelIds )
        {
            recordState.addLabelToNode( labelId, nodeId.get() );
        }

        // Extract the dynamic label record id (which is also a verification that we allocated one)
        NodeRecord node = single( context.getNodeChanges().changes() ).forReadingData();
        dynamicLabelRecordId.set( single( node.getDynamicLabelRecords() ).getId() );

        return recordState;
    }

    private TransactionRecordState deleteNode( NeoStore store, long nodeId )
    {
        TransactionRecordStateContext context = context( store );
        TransactionRecordState recordState = recordState( store, context );
        recordState.nodeDelete( nodeId );
        return recordState;
    }

    private void apply( NeoCommandHandler applier, TransactionRepresentation transaction ) throws IOException
    {
        transaction.accept( new CommandApplierFacade( applier ) );
    }

    private TransactionRecordState recordState( NeoStore store, TransactionRecordStateContext context )
    {
        return new TransactionRecordState( store,
                new IntegrityValidator( store, mock( IndexingService.class ) ), context );
    }

    private TransactionRecordStateContext context( NeoStore store )
    {
        TransactionRecordStateContextSupplier contextSupplier = new TransactionRecordStateContextSupplier( store );
        TransactionRecordStateContext context = new TransactionRecordStateContext( contextSupplier, store );
        return context;
    }

    private TransactionRepresentation transaction( TransactionRecordState recordState )
            throws TransactionFailureException
    {
        List<Command> commands = new ArrayList<>();
        recordState.extractCommands( commands );
        PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( commands );
        transaction.setHeader( new byte[0], 0, 0, 0, 0, 0, 0 );
        return transaction;
    }

    private void assertDynamicLabelRecordInUse( NeoStore store, long id, boolean inUse )
    {
        DynamicRecord record = store.getNodeStore().getDynamicLabelStore().forceGetRaw( id );
        assertTrue( inUse == record.inUse() );
    }

    public final @Rule CleanupRule cleanup = new CleanupRule();
    public final @Rule EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    public final @Rule PageCacheRule pageCacheRule = new PageCacheRule();
}
