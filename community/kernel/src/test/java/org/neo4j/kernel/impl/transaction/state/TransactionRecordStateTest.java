/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CommandApplierFacade;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCommand;
import org.neo4j.kernel.impl.transaction.command.CommandHandler;
import org.neo4j.kernel.impl.transaction.command.NeoStoreTransactionApplier;
import org.neo4j.kernel.impl.transaction.log.CommandWriter;
import org.neo4j.kernel.impl.transaction.log.InMemoryVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.WritableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;

import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.helpers.collection.MapUtil.stringMap;


public class TransactionRecordStateTest
{
    @Rule
    public final CleanupRule cleanup = new CleanupRule();
    @Rule
    public final EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    @Test
    public void shouldDeleteDynamicLabelsForDeletedNode() throws Exception
    {
        // GIVEN a store that has got a node with a dynamic label record
        NeoStores store = newNeoStores();
        CommandHandler applier = new NeoStoreTransactionApplier( store, mock( CacheAccessBackDoor.class ),
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
        NeoStores store = newNeoStores();
        CommandHandler applier = new NeoStoreTransactionApplier( store, mock( CacheAccessBackDoor.class ),
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

    @Test
    public void shouldExtractCreatedCommandsInCorrectOrder() throws Exception
    {
        // GIVEN
        NeoStores neoStores = newNeoStores( GraphDatabaseSettings.dense_node_threshold.name(), "1" );
        NeoStoreTransactionContext context = getNeoStoreTransactionContext( neoStores );
        TransactionRecordState recordState =
                new TransactionRecordState( neoStores, mock( IntegrityValidator.class ), context );
        long nodeId = 0, relId = 1;
        recordState.nodeCreate( nodeId );
        recordState.relCreate( relId++, 0, nodeId, nodeId );
        recordState.relCreate( relId, 0, nodeId, nodeId );
        recordState.nodeAddProperty( nodeId, 0, 101 );

        // WHEN
        Collection<Command> commands = new ArrayList<>();
        recordState.extractCommands( commands );

        // THEN
        Iterator<Command> commandIterator = commands.iterator();

        assertCommand( commandIterator.next(), PropertyCommand.class );
        assertCommand( commandIterator.next(), RelationshipCommand.class );
        assertCommand( commandIterator.next(), RelationshipCommand.class );
        assertCommand( commandIterator.next(), Command.RelationshipGroupCommand.class );
        assertCommand( commandIterator.next(), NodeCommand.class );
        assertFalse( commandIterator.hasNext() );
    }


    @Test
    public void shouldExtractUpdateCommandsInCorrectOrder() throws Exception
    {
        // GIVEN
        NeoStores neoStores = newNeoStores( GraphDatabaseSettings.dense_node_threshold.name(), "1" );
        NeoStoreTransactionContext context = getNeoStoreTransactionContext( neoStores );
        TransactionRecordState recordState =
                new TransactionRecordState( neoStores, mock( IntegrityValidator.class ), context );
        long nodeId = 0, relId1 = 1, relId2 = 2, relId3 = 3;
        recordState.nodeCreate( nodeId );
        recordState.relCreate( relId1, 0, nodeId, nodeId );
        recordState.relCreate( relId2, 0, nodeId, nodeId );
        recordState.nodeAddProperty( nodeId, 0, 101 );
        CommandHandler applier = new NeoStoreTransactionApplier( neoStores, mock( CacheAccessBackDoor.class ),
                LockService.NO_LOCK_SERVICE, new LockGroup(), 1 );
        apply( applier, transaction( recordState ) );

        context = getNeoStoreTransactionContext( neoStores );
        recordState = new TransactionRecordState( neoStores, mock( IntegrityValidator.class ), context );
        recordState.nodeChangeProperty( nodeId, 0, 102 );
        recordState.relCreate( relId3, 0, nodeId, nodeId );
        recordState.relAddProperty( relId1, 0, 123 );

        // WHEN
        Collection<Command> commands = new ArrayList<>();
        recordState.extractCommands( commands );

        // THEN
        Iterator<Command> commandIterator = commands.iterator();

        // added rel property
        assertCommand( commandIterator.next(), PropertyCommand.class );
        // created relationship relId3
        assertCommand( commandIterator.next(), RelationshipCommand.class );
        // rest is updates...
        assertCommand( commandIterator.next(), PropertyCommand.class );
        assertCommand( commandIterator.next(), RelationshipCommand.class );
        assertCommand( commandIterator.next(), RelationshipCommand.class );
        assertCommand( commandIterator.next(), Command.RelationshipGroupCommand.class );
        assertCommand( commandIterator.next(), NodeCommand.class );
        assertFalse( commandIterator.hasNext() );
    }

    @Test
    public void shouldIgnoreRelationshipGroupCommandsForGroupThatIsCreatedAndDeletedInThisTx() throws Exception
    {
        /*
         * This test verifies that there are no transaction commands generated for a state diff that contains a
         * relationship group that is created and deleted in this tx. This case requires special handling because
         * relationship groups can be created and then deleted from disjoint code paths. Look at
         * TransactionRecordState.extractCommands() for more details.
         *
         * The test setup looks complicated but all it does is mock properly a NeoStoreTransactionContext to
         * return an Iterable<RecordSet< that contains a RelationshipGroup record which has been created in this
         * tx and also is set notInUse.
         */
        // Given
        NeoStores neoStore = newNeoStores();
        NeoStoreTransactionContext context = mock( NeoStoreTransactionContext.class, RETURNS_MOCKS );

        RecordAccess<Long, RelationshipGroupRecord, Integer> relGroupRecordsMock = mock( RecordAccess.class );

        Command.RelationshipGroupCommand theCommand = new Command.RelationshipGroupCommand();
        RelationshipGroupRecord theRecord = new RelationshipGroupRecord( 1, 1 );
        theRecord.setInUse( false ); // this is where we set the record to be not in use
        theCommand.init( theRecord );

        LinkedList<RecordAccess.RecordProxy<Long, RelationshipGroupRecord, Integer>> commands =
                new LinkedList<>();

        RecordAccess.RecordProxy<Long, RelationshipGroupRecord, Integer> theProxyMock = mock( RecordAccess.RecordProxy.class );
        when( theProxyMock.isCreated() ).thenReturn( true ); // and this is where it is set to be created in this tx
        when( theProxyMock.forReadingLinkage() ).thenReturn( theRecord );
        commands.add( theProxyMock );

        when( relGroupRecordsMock.changes() ).thenReturn( commands );
        when( context.getRelGroupRecords() ).thenReturn( relGroupRecordsMock );
        when( relGroupRecordsMock.changeSize() ).thenReturn( 1 ); // necessary for passingan assertion in recordState

        TransactionRecordState recordState =
                new TransactionRecordState( neoStore, mock( IntegrityValidator.class ), context );

        // When
        Set<Command> resultingCommands = new HashSet<>();
        recordState.extractCommands( resultingCommands );

        // Then
        assertTrue( resultingCommands.isEmpty() );
    }

    @Test
    public void shouldExtractDeleteCommandsInCorrectOrder() throws Exception
    {
        // GIVEN
        NeoStores neoStores = newNeoStores( GraphDatabaseSettings.dense_node_threshold.name(), "1" );
        NeoStoreTransactionContext context = getNeoStoreTransactionContext( neoStores );
        TransactionRecordState recordState =
                new TransactionRecordState( neoStores, mock( IntegrityValidator.class ), context );
        long nodeId1 = 0, nodeId2 = 1, relId1 = 1, relId2 = 2, relId4 = 10;
        recordState.nodeCreate( nodeId1 );
        recordState.nodeCreate( nodeId2 );
        recordState.relCreate( relId1, 0, nodeId1, nodeId1 );
        recordState.relCreate( relId2, 0, nodeId1, nodeId1 );
        recordState.relCreate( relId4, 1, nodeId1, nodeId1 );
        recordState.nodeAddProperty( nodeId1, 0, 101 );
        CommandHandler applier = new NeoStoreTransactionApplier( neoStores, mock( CacheAccessBackDoor.class ),
                LockService.NO_LOCK_SERVICE, new LockGroup(), 1 );
        apply( applier, transaction( recordState ) );

        context = getNeoStoreTransactionContext( neoStores );
        recordState = new TransactionRecordState( neoStores, mock( IntegrityValidator.class ), context );
        recordState.relDelete( relId4 );
        recordState.nodeDelete( nodeId2 );
        recordState.nodeRemoveProperty( nodeId1, 0 );

        // WHEN
        Collection<Command> commands = new ArrayList<>();
        recordState.extractCommands( commands );

        // THEN
        Iterator<Command> commandIterator = commands.iterator();

        // updated rel group to not point to the deleted one below
        assertCommand( commandIterator.next(), Command.RelationshipGroupCommand.class );
        // updated node to point to the group after the deleted one
        assertCommand( commandIterator.next(), NodeCommand.class );
        // rest is deletions below...
        assertCommand( commandIterator.next(), PropertyCommand.class );
        assertCommand( commandIterator.next(), RelationshipCommand.class );
        assertCommand( commandIterator.next(), Command.RelationshipGroupCommand.class );
        assertCommand( commandIterator.next(), NodeCommand.class );
        assertFalse( commandIterator.hasNext() );
    }

    private void assertCommand( Command next, Class klass )
    {
        assertTrue( "Expected " + klass + ". was: " + next, klass.isInstance( next ) );
    }

    private CommittedTransactionRepresentation readFromChannel( ReadableVersionableLogChannel channel ) throws IOException
    {
        LogEntryReader<ReadableVersionableLogChannel> logEntryReader = new VersionAwareLogEntryReader<>();
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
        TransactionLogWriter writer = new TransactionLogWriter( new LogEntryWriter( channel,
                new CommandWriter( channel ) ) );
        writer.append( transaction, 2 );
    }

    private NeoStores newNeoStores( String... config )
    {
        File storeDir = new File( "dir" );
        EphemeralFileSystemAbstraction fs = fsr.get();
        fs.mkdirs( storeDir );
        Config configuration = new Config( stringMap( config ) );
        StoreFactory storeFactory = new StoreFactory( storeDir, configuration, new DefaultIdGeneratorFactory( fs ),
                pageCacheRule.getPageCache( fs ), fs, NullLogProvider.getInstance() );
        return cleanup.add( storeFactory.openAllNeoStores( true ) );
    }

    private TransactionRecordState nodeWithDynamicLabelRecord( NeoStores store,
            AtomicLong nodeId, AtomicLong dynamicLabelRecordId )
    {
        NeoStoreTransactionContext context = new NeoStoreTransactionContext( store );
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
        NodeRecord node = single( context.getNodeRecords().changes() ).forReadingData();
        dynamicLabelRecordId.set( single( node.getDynamicLabelRecords() ).getId() );

        return recordState;
    }

    private TransactionRecordState deleteNode( NeoStores store, long nodeId )
    {
        NeoStoreTransactionContext context = new NeoStoreTransactionContext( store );
        TransactionRecordState recordState = recordState( store, context );
        recordState.nodeDelete( nodeId );
        return recordState;
    }

    private void apply( CommandHandler applier, TransactionRepresentation transaction ) throws IOException
    {
        transaction.accept( new CommandApplierFacade( applier ) );
    }

    private TransactionRecordState recordState( NeoStores store, NeoStoreTransactionContext context )
    {
        return new TransactionRecordState( store,
                new IntegrityValidator( store, mock( IndexingService.class ) ), context );
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

    private void assertDynamicLabelRecordInUse( NeoStores store, long id, boolean inUse )
    {
        DynamicRecord record = store.getNodeStore().getDynamicLabelStore().forceGetRecord( id );
        assertTrue( inUse == record.inUse() );
    }

    private NeoStoreTransactionContext getNeoStoreTransactionContext( NeoStores neoStores )
    {
        NeoStoreTransactionContext context = new NeoStoreTransactionContext( neoStores );
        context.init( mock( Locks.Client.class ) );
        return context;
    }
}
