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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.api.BatchTransactionApplier;
import org.neo4j.kernel.impl.api.CommandVisitor;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.NodePropertyCommandsExtractor;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCommand;
import org.neo4j.kernel.impl.transaction.command.CommandHandlerContract;
import org.neo4j.kernel.impl.transaction.command.NeoStoreBatchTransactionApplier;
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
import org.neo4j.test.NeoStoresRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.add;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.change;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.remove;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.store.record.IndexRule.indexRule;
import static org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule.uniquenessConstraintRule;

public class TransactionRecordStateTest
{
    private static final String LONG_STRING = "string value long enough not to be stored as a short string";

    public static void assertRelationshipGroupDoesNotExist( NeoStoreTransactionContext txCtx, NodeRecord node,
            int type )
    {
        assertNull( txCtx.getRelationshipGroup( node, type ) );
    }

    public static void assertDenseRelationshipCounts( TransactionRecordState tx, NeoStoreTransactionContext txCtx,
            long nodeId, int type, int outCount, int inCount )
    {
        RelationshipGroupRecord group = txCtx.getRelationshipGroup(
                txCtx.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(), type ).forReadingData();
        assertNotNull( group );

        RelationshipRecord rel;
        long relId = group.getFirstOut();
        if ( relId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            rel = txCtx.getRelRecords().getOrLoad( relId, null ).forReadingData();
            // count is stored in the back pointer of the first relationship in the chain
            assertEquals( "Stored relationship count for OUTGOING differs", outCount, rel.getFirstPrevRel() );
            assertEquals( "Manually counted relationships for OUTGOING differs", outCount,
                    manuallyCountRelationships( txCtx, nodeId, relId ) );
        }

        relId = group.getFirstIn();
        if ( relId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            rel = txCtx.getRelRecords().getOrLoad( relId, null ).forReadingData();
            assertEquals( "Stored relationship count for INCOMING differs", inCount, rel.getSecondPrevRel() );
            assertEquals( "Manually counted relationships for INCOMING differs", inCount,
                    manuallyCountRelationships( txCtx, nodeId, relId ) );
        }
    }

    private static int manuallyCountRelationships( NeoStoreTransactionContext txCtx, long nodeId, long firstRelId )
    {
        int count = 0;
        long relId = firstRelId;
        while ( relId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            count++;
            RelationshipRecord record = txCtx.getRelRecords().getOrLoad( relId, null ).forReadingData();
            relId = record.getFirstNode() == nodeId ? record.getFirstNextRel() : record.getSecondNextRel();
        }
        return count;
    }

    @Rule
    public final NeoStoresRule neoStoresRule = new NeoStoresRule( getClass() );
    private final IntegrityValidator integrityValidator = mock( IntegrityValidator.class );

    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldCreateEqualNodePropertyUpdatesOnRecoveryOfCreatedNode() throws Exception
    {
        /* There was an issue where recovering a tx where a node with a label and a property
         * was created resulted in two exact copies of NodePropertyUpdates. */

        // GIVEN
        long nodeId = 0;
        int labelId = 5, propertyKeyId = 7;

        // -- an index
        long ruleId = 0;
        NeoStores neoStores = neoStoresRule.open();
        TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                newContext( neoStores ) );
        SchemaRule rule = indexRule( ruleId, labelId, propertyKeyId, PROVIDER_DESCRIPTOR );
        recordState.createSchemaRule( rule );
        apply( neoStores, recordState );

        // -- and a tx creating a node with that label and property key
        recordState = new TransactionRecordState( neoStores, integrityValidator, newContext( neoStores ) );
        recordState.nodeCreate( nodeId );
        recordState.addLabelToNode( labelId, nodeId );
        recordState.nodeAddProperty( nodeId, propertyKeyId, "Neo" );

        // WHEN
        PhysicalTransactionRepresentation transaction = transactionRepresentationOf( recordState );
        NodePropertyCommandsExtractor extractor = new NodePropertyCommandsExtractor();
        transaction.accept( extractor );

        // THEN
        // -- later recovering that tx, there should be only one update
        assertTrue( extractor.containsAnyNodeOrPropertyUpdate() );
        PrimitiveLongSet recoveredNodeIds = Primitive.longSet();
        recoveredNodeIds.addAll( extractor.nodeCommandsById().iterator() );
        recoveredNodeIds.addAll( extractor.propertyCommandsByNodeIds().iterator() );
        assertEquals( 1, recoveredNodeIds.size() );
        assertEquals( nodeId, recoveredNodeIds.iterator().next() );
    }

    @Test
    public void shouldWriteProperPropertyRecordsWhenOnlyChangingLinkage() throws Exception
    {
        /* There was an issue where GIVEN:
         *
         *   Legend: () = node, [] = property record
         *
         *   ()-->[0:block{size:1}]
         *
         * WHEN adding a new property record in front of if, not changing any data in that record i.e:
         *
         *   ()-->[1:block{size:4}]-->[0:block{size:1}]
         *
         * The state of property record 0 would be that it had loaded value records for that block,
         * but those value records weren't heavy, so writing that record to the log would fail
         * w/ an assertion data != null.
         */

        // GIVEN
        NeoStores neoStores = neoStoresRule.open();
        TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                newContext( neoStores ) );
        int nodeId = 0;
        recordState.nodeCreate( nodeId );
        int index = 0;
        recordState.nodeAddProperty( nodeId, index, string( 70 ) ); // will require a block of size 1
        apply( neoStores, recordState );

        // WHEN
        recordState = new TransactionRecordState( neoStores, integrityValidator, newContext( neoStores ) );
        int index2 = 1;
        recordState.nodeAddProperty( nodeId, index2, string( 40 ) ); // will require a block of size 4

        // THEN
        PhysicalTransactionRepresentation representation = transactionRepresentationOf( recordState );
        representation.accept( command -> command.handle( new CommandVisitor.Adapter()
        {
            @Override
            public boolean visitPropertyCommand( PropertyCommand command ) throws IOException
            {
                // THEN
                verifyPropertyRecord( command.getBefore() );
                verifyPropertyRecord( command.getAfter() );
                return false;
            }

            private void verifyPropertyRecord( PropertyRecord record )
            {
                if ( record.getPrevProp() != Record.NO_NEXT_PROPERTY.intValue() )
                {
                    for ( PropertyBlock block : record )
                    {
                        assertTrue( block.isLight() );
                    }
                }
            }
        } ) );
    }

    @Test
    public void shouldConvertLabelAdditionToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 0;
        NeoStores neoStores = neoStoresRule.open();
        TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                newContext( neoStores ) );
        int propertyKey1 = 1, propertyKey2 = 2, labelId = 3;
        long[] labelIds = new long[]{labelId};
        Object value1 = LONG_STRING, value2 = LONG_STRING.getBytes();
        recordState.nodeCreate( nodeId );
        recordState.nodeAddProperty( nodeId, propertyKey1, value1 );
        recordState.nodeAddProperty( nodeId, propertyKey2, value2 );
        apply( neoStores, recordState );

        // WHEN
        recordState = new TransactionRecordState( neoStores, integrityValidator, newContext( neoStores ) );
        recordState.addLabelToNode( labelId, nodeId );
        Iterable<NodePropertyUpdate> indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        assertEquals(
                asSet( add( nodeId, propertyKey1, value1, labelIds ), add( nodeId, propertyKey2, value2, labelIds ) ),

                asSet( indexUpdates ) );
    }

    @Test
    public void shouldConvertMixedLabelAdditionAndSetPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 0;
        NeoStores neoStores = neoStoresRule.open();
        TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                newContext( neoStores ) );
        int propertyKey1 = 1, propertyKey2 = 2, labelId1 = 3, labelId2 = 4;
        Object value1 = "first", value2 = 4;
        recordState.nodeCreate( nodeId );
        recordState.nodeAddProperty( nodeId, propertyKey1, value1 );
        recordState.addLabelToNode( labelId1, nodeId );
        apply( neoStores, recordState );

        // WHEN
        recordState = new TransactionRecordState( neoStores, integrityValidator, newContext( neoStores ) );
        recordState.nodeAddProperty( nodeId, propertyKey2, value2 );
        recordState.addLabelToNode( labelId2, nodeId );
        Iterable<NodePropertyUpdate> indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        assertEquals( asSet( add( nodeId, propertyKey1, value1, new long[]{labelId2} ),
                add( nodeId, propertyKey2, value2, new long[]{labelId2} ),
                add( nodeId, propertyKey2, value2, new long[]{labelId1} ) ), asSet( indexUpdates ) );
    }

    @Test
    public void shouldConvertLabelRemovalToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 0;
        NeoStores neoStores = neoStoresRule.open();
        TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                newContext( neoStores ) );
        int propertyKey1 = 1, propertyKey2 = 2, labelId = 3;
        long[] labelIds = new long[]{labelId};
        Object value1 = "first", value2 = 4;
        recordState.nodeCreate( nodeId );
        recordState.nodeAddProperty( nodeId, propertyKey1, value1 );
        recordState.nodeAddProperty( nodeId, propertyKey2, value2 );
        recordState.addLabelToNode( labelId, nodeId );
        apply( neoStores, recordState );

        // WHEN
        recordState = new TransactionRecordState( neoStores, integrityValidator, newContext( neoStores ) );
        recordState.removeLabelFromNode( labelId, nodeId );
        Iterable<NodePropertyUpdate> indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        assertEquals( asSet( remove( nodeId, propertyKey1, value1, labelIds ),
                remove( nodeId, propertyKey2, value2, labelIds ) ),

                asSet( indexUpdates ) );
    }

    @Test
    public void shouldConvertMixedLabelRemovalAndRemovePropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 0;
        NeoStores neoStores = neoStoresRule.open();
        TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                newContext( neoStores ) );
        int propertyKey1 = 1, propertyKey2 = 2, labelId1 = 3, labelId2 = 4;
        Object value1 = "first", value2 = 4;
        recordState.nodeCreate( nodeId );
        DefinedProperty property1 = recordState.nodeAddProperty( nodeId, propertyKey1, value1 );
        recordState.nodeAddProperty( nodeId, propertyKey2, value2 );
        recordState.addLabelToNode( labelId1, nodeId );
        recordState.addLabelToNode( labelId2, nodeId );
        apply( neoStores, recordState );

        // WHEN
        recordState = new TransactionRecordState( neoStores, integrityValidator, newContext( neoStores ) );
        recordState.nodeRemoveProperty( nodeId, property1.propertyKeyId() );
        recordState.removeLabelFromNode( labelId2, nodeId );
        Iterable<NodePropertyUpdate> indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        assertEquals( asSet( remove( nodeId, propertyKey1, value1, new long[]{labelId1, labelId2} ),
                remove( nodeId, propertyKey2, value2, new long[]{labelId2} ) ),

                asSet( indexUpdates ) );
    }

    @Test
    public void shouldConvertMixedLabelRemovalAndAddPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 0;
        NeoStores neoStores = neoStoresRule.open();
        TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                newContext( neoStores ) );
        int propertyKey1 = 1, propertyKey2 = 2, labelId1 = 3, labelId2 = 4;
        Object value1 = "first", value2 = 4;
        recordState.nodeCreate( nodeId );
        recordState.nodeAddProperty( nodeId, propertyKey1, value1 );
        recordState.addLabelToNode( labelId1, nodeId );
        recordState.addLabelToNode( labelId2, nodeId );
        apply( neoStores, recordState );

        // WHEN
        recordState = new TransactionRecordState( neoStores, integrityValidator, newContext( neoStores ) );
        recordState.nodeAddProperty( nodeId, propertyKey2, value2 );
        recordState.removeLabelFromNode( labelId2, nodeId );
        Iterable<NodePropertyUpdate> indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        assertEquals( asSet( add( nodeId, propertyKey2, value2, new long[]{labelId1} ),
                remove( nodeId, propertyKey1, value1, new long[]{labelId2} ),
                remove( nodeId, propertyKey2, value2, new long[]{labelId2} ) ),

                asSet( indexUpdates ) );
    }

    @Test
    public void shouldConvertChangedPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        int nodeId = 0;
        NeoStores neoStores = neoStoresRule.open();
        TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                newContext( neoStores ) );
        int propertyKey1 = 1, propertyKey2 = 2;
        Object value1 = "first", value2 = 4;
        recordState.nodeCreate( nodeId );
        DefinedProperty property1 = recordState.nodeAddProperty( nodeId, propertyKey1, value1 );
        DefinedProperty property2 = recordState.nodeAddProperty( nodeId, propertyKey2, value2 );
        apply( neoStores, transactionRepresentationOf( recordState ) );

        // WHEN
        Object newValue1 = "new", newValue2 = "new 2";
        recordState = new TransactionRecordState( neoStores, integrityValidator, newContext( neoStores ) );
        recordState.nodeChangeProperty( nodeId, property1.propertyKeyId(), newValue1 );
        recordState.nodeChangeProperty( nodeId, property2.propertyKeyId(), newValue2 );
        Iterable<NodePropertyUpdate> indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        assertEquals( asSet( change( nodeId, propertyKey1, value1, EMPTY_LONG_ARRAY, newValue1, EMPTY_LONG_ARRAY ),
                change( nodeId, propertyKey2, value2, EMPTY_LONG_ARRAY, newValue2, EMPTY_LONG_ARRAY ) ),

                asSet( indexUpdates ) );
    }

    @Test
    public void shouldConvertRemovedPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        int nodeId = 0;
        NeoStores neoStores = neoStoresRule.open();
        TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                newContext( neoStores ) );
        int propertyKey1 = 1, propertyKey2 = 2;
        int labelId = 3;
        Object value1 = "first", value2 = 4;
        recordState.nodeCreate( nodeId );
        recordState.addLabelToNode( labelId, nodeId );
        DefinedProperty property1 = recordState.nodeAddProperty( nodeId, propertyKey1, value1 );
        DefinedProperty property2 = recordState.nodeAddProperty( nodeId, propertyKey2, value2 );
        apply( neoStores, transactionRepresentationOf( recordState ) );

        // WHEN
        recordState = new TransactionRecordState( neoStores, integrityValidator, newContext( neoStores ) );
        recordState.nodeRemoveProperty( nodeId, property1.propertyKeyId() );
        recordState.nodeRemoveProperty( nodeId, property2.propertyKeyId() );
        Iterable<NodePropertyUpdate> indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        assertEquals( asSet( remove( nodeId, propertyKey1, value1, new long[]{labelId} ),
                remove( nodeId, propertyKey2, value2, new long[]{labelId} ) ), asSet( indexUpdates ) );
    }

    @Test
    public void shouldDeleteDynamicLabelsForDeletedNode() throws Throwable
    {
        // GIVEN a store that has got a node with a dynamic label record
        NeoStores store = neoStoresRule.open();
        BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( store, mock( CacheAccessBackDoor.class ),
                LockService.NO_LOCK_SERVICE );
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
    public void shouldDeleteDynamicLabelsForDeletedNodeForRecoveredTransaction() throws Throwable
    {
        // GIVEN a store that has got a node with a dynamic label record
        NeoStores store = neoStoresRule.open();
        BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( store, mock( CacheAccessBackDoor.class ),
                LockService.NO_LOCK_SERVICE );
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
    public void shouldExtractCreatedCommandsInCorrectOrder() throws Throwable
    {
        // GIVEN
        NeoStores neoStores = neoStoresRule.open( GraphDatabaseSettings.dense_node_threshold.name(), "1" );
        TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                newContext( neoStores ) );
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
    public void shouldExtractUpdateCommandsInCorrectOrder() throws Throwable
    {
        // GIVEN
        NeoStores neoStores = neoStoresRule.open( GraphDatabaseSettings.dense_node_threshold.name(), "1" );
        TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                newContext( neoStores ) );
        long nodeId = 0, relId1 = 1, relId2 = 2, relId3 = 3;
        recordState.nodeCreate( nodeId );
        recordState.relCreate( relId1, 0, nodeId, nodeId );
        recordState.relCreate( relId2, 0, nodeId, nodeId );
        recordState.nodeAddProperty( nodeId, 0, 101 );
        BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( neoStores, mock( CacheAccessBackDoor.class ),
                LockService.NO_LOCK_SERVICE );
        apply( applier, transaction( recordState ) );

        recordState = new TransactionRecordState( neoStores, mock( IntegrityValidator.class ),
                newContext( neoStores ) );
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
    public void shouldExtractDeleteCommandsInCorrectOrder() throws Throwable
    {
        // GIVEN
        NeoStores neoStores = neoStoresRule.open( GraphDatabaseSettings.dense_node_threshold.name(), "1" );
        TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                newContext( neoStores ) );
        long nodeId1 = 0, nodeId2 = 1, relId1 = 1, relId2 = 2, relId4 = 10;
        recordState.nodeCreate( nodeId1 );
        recordState.nodeCreate( nodeId2 );
        recordState.relCreate( relId1, 0, nodeId1, nodeId1 );
        recordState.relCreate( relId2, 0, nodeId1, nodeId1 );
        recordState.relCreate( relId4, 1, nodeId1, nodeId1 );
        recordState.nodeAddProperty( nodeId1, 0, 101 );
        BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( neoStores, mock( CacheAccessBackDoor.class ),
                LockService.NO_LOCK_SERVICE );
        apply( applier, transaction( recordState ) );

        recordState = new TransactionRecordState( neoStores, mock( IntegrityValidator.class ),
                newContext( neoStores ) );
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

    @Test
    public void shouldValidateConstraintIndexAsPartOfExtraction() throws Throwable
    {
        // GIVEN
        NeoStores neoStores = neoStoresRule.open();
        TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                newContext( neoStores ) );

        final long indexId = neoStores.getSchemaStore().nextId();
        final long constraintId = neoStores.getSchemaStore().nextId();

        recordState.createSchemaRule( uniquenessConstraintRule( constraintId, 1, 1, indexId ) );

        // WHEN
        recordState.extractCommands( new ArrayList<Command>() );

        // THEN
        verify( integrityValidator ).validateSchemaRule( any() );
    }

    @Test
    public void shouldCreateProperBeforeAndAfterPropertyCommandsWhenAddingProperty() throws Exception
    {
        // GIVEN
        NeoStores neoStores = neoStoresRule.open();
        TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                newContext( neoStores ) );

        int nodeId = 1;
        recordState.nodeCreate( nodeId );
        int propertyKey = 1;
        Object value = 5;

        // WHEN
        recordState.nodeAddProperty( nodeId, propertyKey, value );
        Collection<Command> commands = new ArrayList<>();
        recordState.extractCommands( commands );
        PropertyCommand propertyCommand = singlePropertyCommand( commands );

        // THEN
        PropertyRecord before = propertyCommand.getBefore();
        assertFalse( before.inUse() );
        assertFalse( before.iterator().hasNext() );

        PropertyRecord after = propertyCommand.getAfter();
        assertTrue( after.inUse() );
        assertEquals( 1, count( after ) );
    }

    @Test
    public void shouldConvertAddedPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 0;
        NeoStores neoStores = neoStoresRule.open();
        TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                newContext( neoStores ) );
        int labelId = 3;
        int propertyKey1 = 1, propertyKey2 = 2;
        Object value1 = "first", value2 = 4;

        // WHEN
        recordState.nodeCreate( nodeId );
        recordState.addLabelToNode( labelId, nodeId );
        recordState.nodeAddProperty( nodeId, propertyKey1, value1 );
        recordState.nodeAddProperty( nodeId, propertyKey2, value2 );
        Iterable<NodePropertyUpdate> updates = indexUpdatesOf( neoStores, recordState );

        // THEN
        assertEquals( asSet( add( nodeId, propertyKey1, value1, new long[]{labelId} ),
                add( nodeId, propertyKey2, value2, new long[]{labelId} ) ), asSet( updates ) );
    }

    @Test
    public void shouldLockUpdatedNodes() throws Exception
    {
        // given
        LockService locks = mock( LockService.class, new Answer()
        {
            @Override
            public synchronized Object answer( final InvocationOnMock invocation ) throws Throwable
            {
                // This is necessary because finalize() will also be called
                String name = invocation.getMethod().getName();
                if ( name.equals( "acquireNodeLock" ) || name.equals( "acquireRelationshipLock" ) )
                {
                    final Lock mock = mock( Lock.class, new Answer()
                    {
                        @Override
                        public Object answer( InvocationOnMock invocationOnMock ) throws Throwable
                        {
                            return null;
                        }
                    } );
                    return mock;
                }
                else
                {
                    return null;
                }
            }
        } );
        NeoStores neoStores = neoStoresRule.open();
        NodeStore nodeStore = neoStores.getNodeStore();
        long[] nodes = { // allocate ids
                nodeStore.nextId(), nodeStore.nextId(), nodeStore.nextId(), nodeStore.nextId(), nodeStore.nextId(),
                nodeStore.nextId(), nodeStore.nextId(),};

        {
            // create the node records that we will modify in our main tx.
            TransactionRecordState tx = recordState( neoStores, newContext( neoStores ) );
            for ( int i = 1; i < nodes.length - 1; i++ )
            {
                tx.nodeCreate( nodes[i] );
            }
            tx.nodeAddProperty( nodes[3], 0, "old" );
            tx.nodeAddProperty( nodes[4], 0, "old" );
            BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( neoStores, mock( CacheAccessBackDoor.class ),
                    locks );
            apply( applier, transaction( tx ) );
        }
        reset( locks );

        // These are the changes we want to assert locking on
        TransactionRecordState tx = recordState( neoStores, newContext( neoStores ) );
        tx.nodeCreate( nodes[0] );
        tx.addLabelToNode( 0, nodes[1] );
        tx.nodeAddProperty( nodes[2], 0, "value" );
        tx.nodeChangeProperty( nodes[3], 0, "value" );
        tx.nodeRemoveProperty( nodes[4], 0 );
        tx.nodeDelete( nodes[5] );

        tx.nodeCreate( nodes[6] );
        tx.addLabelToNode( 0, nodes[6] );
        tx.nodeAddProperty( nodes[6], 0, "value" );

        //commit( tx );
        BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( neoStores, mock( CacheAccessBackDoor.class ), locks );
        apply( applier, transaction( tx ) );

        // then
        // create node, NodeCommand == 1 update
        verify( locks, times( 1 ) ).acquireNodeLock( nodes[0], LockService.LockType.WRITE_LOCK );
        // add label, NodeCommand == 1 update
        verify( locks, times( 1 ) ).acquireNodeLock( nodes[1], LockService.LockType.WRITE_LOCK );
        // add property, NodeCommand and PropertyCommand == 2 updates
        verify( locks, times( 2 ) ).acquireNodeLock( nodes[2], LockService.LockType.WRITE_LOCK );
        // update property, in place, PropertyCommand == 1 update
        verify( locks, times( 1 ) ).acquireNodeLock( nodes[3], LockService.LockType.WRITE_LOCK );
        // remove property, updates the Node and the Property == 2 updates
        verify( locks, times( 2 ) ).acquireNodeLock( nodes[4], LockService.LockType.WRITE_LOCK );
        // delete node, single NodeCommand == 1 update
        verify( locks, times( 1 ) ).acquireNodeLock( nodes[5], LockService.LockType.WRITE_LOCK );
        // create and add-label goes into the NodeCommand, add property is a PropertyCommand == 2 updates
        verify( locks, times( 2 ) ).acquireNodeLock( nodes[6], LockService.LockType.WRITE_LOCK );
    }

    @Test
    public void movingBilaterallyOfTheDenseNodeThresholdIsConsistent() throws Exception
    {
        // GIVEN
        NeoStores neoStores = neoStoresRule.open( GraphDatabaseSettings.dense_node_threshold.name(), "10" );
        TransactionRecordState tx = recordState( neoStores, newContext( neoStores ) );
        long nodeId = neoStores.getNodeStore().nextId();

        tx.nodeCreate( nodeId );

        int typeA = (int) neoStores.getRelationshipTypeTokenStore().nextId();
        tx.createRelationshipTypeToken( "A", typeA );
        createRelationships( neoStores, tx, nodeId, typeA, INCOMING, 20 );

        BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( neoStores, mock( CacheAccessBackDoor.class ),
                LockService.NO_LOCK_SERVICE );
        apply( applier, transaction( tx ) );

        tx = recordState( neoStores, newContext( neoStores ) );

        int typeB = 1;
        tx.createRelationshipTypeToken( "B", typeB );


        // WHEN
        // i remove enough relationships to become dense and remove enough to become not dense
        long[] relationshipsOfTypeB = createRelationships( neoStores, tx, nodeId, typeB, OUTGOING, 5 );
        for ( long relationshipToDelete : relationshipsOfTypeB )
        {
            tx.relDelete( relationshipToDelete );
        }

        PhysicalTransactionRepresentation ptx = transactionRepresentationOf( tx );
        apply( applier, ptx );

        // THEN
        // The dynamic label record in before should be the same id as in after, and should be in use
        final AtomicBoolean foundRelationshipGroupInUse = new AtomicBoolean();

        ptx.accept( command -> command.handle( new CommandVisitor.Adapter()
        {
            @Override
            public boolean visitRelationshipGroupCommand( Command.RelationshipGroupCommand command ) throws IOException
            {
                if ( command.getAfter().inUse() )
                {
                    if ( !foundRelationshipGroupInUse.get() )
                    {
                        foundRelationshipGroupInUse.set( true );
                    }
                    else
                    {
                        fail();
                    }
                }
                return false;
            }
        } ) );

        assertTrue( "Did not create relationship group command", foundRelationshipGroupInUse.get() );
    }

    @Test
    public void createdSchemaRuleRecordMustBeWrittenHeavy() throws Exception
    {
        // GIVEN
        NeoStores neoStores = neoStoresRule.open();
        TransactionRecordState tx = recordState( neoStores, newContext( neoStores ) );
        long ruleId = 0;
        int labelId = 5, propertyKeyId = 7;
        SchemaRule rule = new IndexRule( ruleId, labelId, propertyKeyId,
                new SchemaIndexProvider.Descriptor( "quantum-dex", "25.0" ), null );

        // WHEN
        tx.createSchemaRule( rule );
        PhysicalTransactionRepresentation transactionCommands = transactionRepresentationOf( tx );

        transactionCommands.accept( command -> command.handle( new CommandVisitor.Adapter()
        {
            @Override
            public boolean visitSchemaRuleCommand( Command.SchemaRuleCommand command ) throws IOException
            {
                for ( DynamicRecord record : command.getRecordsAfter() )
                {
                    assertFalse( record + " should have been heavy", record.isLight() );
                }
                return false;
            }
        } ) );
    }

    @Test
    public void shouldConvertToDenseNodeRepresentationWhenHittingThresholdWithDifferentTypes() throws Exception
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        NeoStores neoStores = neoStoresRule.open( GraphDatabaseSettings.dense_node_threshold.name(), "50" );
        NeoStoreTransactionContext txCtx = newContext( neoStores );
        TransactionRecordState tx = recordState( neoStores, txCtx );
        long nodeId = neoStores.getNodeStore().nextId();
        int typeA = 0, typeB = 1, typeC = 2;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( "A", typeA );
        createRelationships( neoStores, tx, nodeId, typeA, OUTGOING, 6 );
        createRelationships( neoStores, tx, nodeId, typeA, INCOMING, 7 );

        tx.createRelationshipTypeToken( "B", typeB );
        createRelationships( neoStores, tx, nodeId, typeB, OUTGOING, 8 );
        createRelationships( neoStores, tx, nodeId, typeB, INCOMING, 9 );

        tx.createRelationshipTypeToken( "C", typeC );
        createRelationships( neoStores, tx, nodeId, typeC, OUTGOING, 10 );
        createRelationships( neoStores, tx, nodeId, typeC, INCOMING, 10 );
        // here we're at the edge
        assertFalse( txCtx.getNodeRecords().getOrLoad( nodeId, null ).forReadingData().isDense() );

        // WHEN creating the relationship that pushes us over the threshold
        createRelationships( neoStores, tx, nodeId, typeC, INCOMING, 1 );

        // THEN the node should have been converted into a dense node
        assertTrue( txCtx.getNodeRecords().getOrLoad( nodeId, null ).forReadingData().isDense() );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeA, 6, 7 );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeB, 8, 9 );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeC, 10, 11 );
    }

    @Test
    public void shouldConvertToDenseNodeRepresentationWhenHittingThresholdWithTheSameTypeDifferentDirection()
            throws Exception
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        NeoStores neoStores = neoStoresRule.open( GraphDatabaseSettings.dense_node_threshold.name(), "49" );
        NeoStoreTransactionContext txCtx = newContext( neoStores );
        TransactionRecordState tx = recordState( neoStores, txCtx );
        long nodeId = neoStores.getNodeStore().nextId();
        int typeA = 0;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( "A", typeA );
        createRelationships( neoStores, tx, nodeId, typeA, OUTGOING, 24 );
        createRelationships( neoStores, tx, nodeId, typeA, INCOMING, 25 );

        // here we're at the edge
        assertFalse( txCtx.getNodeRecords().getOrLoad( nodeId, null ).forReadingData().isDense() );

        // WHEN creating the relationship that pushes us over the threshold
        createRelationships( neoStores, tx, nodeId, typeA, INCOMING, 1 );

        // THEN the node should have been converted into a dense node
        assertTrue( txCtx.getNodeRecords().getOrLoad( nodeId, null ).forReadingData().isDense() );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeA, 24, 26 );
    }

    @Test
    public void shouldConvertToDenseNodeRepresentationWhenHittingThresholdWithTheSameTypeSameDirection()
            throws Exception
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        NeoStores neoStores = neoStoresRule.open( GraphDatabaseSettings.dense_node_threshold.name(), "8" );
        NeoStoreTransactionContext txCtx = newContext( neoStores );
        TransactionRecordState tx = recordState( neoStores, txCtx );
        long nodeId = neoStores.getNodeStore().nextId();
        int typeA = 0;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( "A", typeA );
        createRelationships( neoStores, tx, nodeId, typeA, OUTGOING, 8 );

        // here we're at the edge
        assertFalse( txCtx.getNodeRecords().getOrLoad( nodeId, null ).forReadingData().isDense() );

        // WHEN creating the relationship that pushes us over the threshold
        createRelationships( neoStores, tx, nodeId, typeA, OUTGOING, 1 );

        // THEN the node should have been converted into a dense node
        assertTrue( txCtx.getNodeRecords().getOrLoad( nodeId, null ).forReadingData().isDense() );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeA, 9, 0 );
    }

    @Test
    public void shouldMaintainCorrectDataWhenDeletingFromDenseNodeWithOneType() throws Exception
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        NeoStores neoStores = neoStoresRule.open( GraphDatabaseSettings.dense_node_threshold.name(), "13" );
        NeoStoreTransactionContext txCtx = newContext( neoStores );
        TransactionRecordState tx = recordState( neoStores, txCtx );
        int nodeId = (int) neoStores.getNodeStore().nextId(), typeA = 0;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( "A", typeA );
        long[] relationshipsCreated = createRelationships( neoStores, tx, nodeId, typeA, INCOMING, 15 );

        //WHEN
        tx.relDelete( relationshipsCreated[0] );

        // THEN the node should have been converted into a dense node
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeA, 0, 14 );
    }

    @Test
    public void shouldMaintainCorrectDataWhenDeletingFromDenseNodeWithManyTypes() throws Exception
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        NeoStores neoStores = neoStoresRule.open( GraphDatabaseSettings.dense_node_threshold.name(), "1" );
        NeoStoreTransactionContext txCtx = newContext( neoStores );
        TransactionRecordState tx = recordState( neoStores, txCtx );
        long nodeId = neoStores.getNodeStore().nextId();
        int typeA = 0, typeB = 12, typeC = 600;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( "A", typeA );
        long[] relationshipsCreatedAIncoming = createRelationships( neoStores, tx, nodeId, typeA, INCOMING, 1 );
        long[] relationshipsCreatedAOutgoing = createRelationships( neoStores, tx, nodeId, typeA, OUTGOING, 1 );

        tx.createRelationshipTypeToken( "B", typeB );
        long[] relationshipsCreatedBIncoming = createRelationships( neoStores, tx, nodeId, typeB, INCOMING, 1 );
        long[] relationshipsCreatedBOutgoing = createRelationships( neoStores, tx, nodeId, typeB, OUTGOING, 1 );

        tx.createRelationshipTypeToken( "C", typeC );
        long[] relationshipsCreatedCIncoming = createRelationships( neoStores, tx, nodeId, typeC, INCOMING, 1 );
        long[] relationshipsCreatedCOutgoing = createRelationships( neoStores, tx, nodeId, typeC, OUTGOING, 1 );

        // WHEN
        tx.relDelete( relationshipsCreatedAIncoming[0] );

        // THEN
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeA, 1, 0 );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeB, 1, 1 );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeC, 1, 1 );

        // WHEN
        tx.relDelete( relationshipsCreatedAOutgoing[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( txCtx, txCtx.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeA );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeB, 1, 1 );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeC, 1, 1 );

        // WHEN
        tx.relDelete( relationshipsCreatedBIncoming[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( txCtx, txCtx.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeA );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeB, 1, 0 );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeC, 1, 1 );

        // WHEN
        tx.relDelete( relationshipsCreatedBOutgoing[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( txCtx, txCtx.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeA );
        assertRelationshipGroupDoesNotExist( txCtx, txCtx.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeB );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeC, 1, 1 );

        // WHEN
        tx.relDelete( relationshipsCreatedCIncoming[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( txCtx, txCtx.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeA );
        assertRelationshipGroupDoesNotExist( txCtx, txCtx.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeB );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeC, 1, 0 );

        // WHEN
        tx.relDelete( relationshipsCreatedCOutgoing[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( txCtx, txCtx.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeA );
        assertRelationshipGroupDoesNotExist( txCtx, txCtx.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeB );
        assertRelationshipGroupDoesNotExist( txCtx, txCtx.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeC );
    }

    @Test
    public void shouldSortRelationshipGroups() throws Throwable
    {
        // GIVEN
        int type5 = 5, type10 = 10, type15 = 15;
        NeoStores neoStores = neoStoresRule.open( GraphDatabaseSettings.dense_node_threshold.name(), "1" );
        {
            TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                    newContext( neoStores ) );
            neoStores.getRelationshipTypeTokenStore().setHighId( 16 );

            recordState.createRelationshipTypeToken( "5", type5 );
            recordState.createRelationshipTypeToken( "10", type10 );
            recordState.createRelationshipTypeToken( "15", type15 );
            BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( neoStores, mock( CacheAccessBackDoor.class ),
                    LockService.NO_LOCK_SERVICE );
            apply( applier, transaction( recordState ) );
        }

        long nodeId = neoStores.getNodeStore().nextId();
        {
            long otherNode1Id = neoStores.getNodeStore().nextId();
            long otherNode2Id = neoStores.getNodeStore().nextId();
            TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                    newContext( neoStores ) );
            recordState.nodeCreate( nodeId );
            recordState.nodeCreate( otherNode1Id );
            recordState.nodeCreate( otherNode2Id );
            recordState.relCreate( neoStores.getRelationshipStore().nextId(), type10, nodeId, otherNode1Id );
            // This relationship will cause the switch to dense
            recordState.relCreate( neoStores.getRelationshipStore().nextId(), type10, nodeId, otherNode2Id );

            BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( neoStores, mock( CacheAccessBackDoor.class ),
                    LockService.NO_LOCK_SERVICE );
            apply( applier, transaction( recordState ) );

            // Just a little validation of assumptions
            assertRelationshipGroupsInOrder( neoStores, nodeId, type10 );
        }

        // WHEN inserting a relationship of type 5
        {
            TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                    newContext( neoStores ) );
            long otherNodeId = neoStores.getNodeStore().nextId();
            recordState.nodeCreate( otherNodeId );
            recordState.relCreate( neoStores.getRelationshipStore().nextId(), type5, nodeId, otherNodeId );
            BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( neoStores, mock( CacheAccessBackDoor.class ),
                    LockService.NO_LOCK_SERVICE );
            apply( applier, transaction( recordState ) );

            // THEN that group should end up first in the chain
            assertRelationshipGroupsInOrder( neoStores, nodeId, type5, type10 );
        }


        // WHEN inserting a relationship of type 15
        {
            TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                    newContext( neoStores ) );
            long otherNodeId = neoStores.getNodeStore().nextId();
            recordState.nodeCreate( otherNodeId );
            recordState.relCreate( neoStores.getRelationshipStore().nextId(), type15, nodeId, otherNodeId );
            BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( neoStores, mock( CacheAccessBackDoor.class ),
                    LockService.NO_LOCK_SERVICE );
            apply( applier, transaction( recordState ) );

            // THEN that group should end up last in the chain
            assertRelationshipGroupsInOrder( neoStores, nodeId, type5, type10, type15 );
        }
    }

    private long[] createRelationships( NeoStores neoStores, TransactionRecordState tx, long nodeId, int type,
            Direction direction, int count )
    {
        long[] result = new long[count];
        for ( int i = 0; i < count; i++ )
        {
            long otherNodeId = neoStores.getNodeStore().nextId();
            tx.nodeCreate( otherNodeId );
            long first = direction == OUTGOING ? nodeId : otherNodeId;
            long other = direction == INCOMING ? nodeId : otherNodeId;
            long relId = neoStores.getRelationshipStore().nextId();
            result[i] = relId;
            tx.relCreate( relId, type, first, other );
        }
        return result;
    }

    private void assertRelationshipGroupsInOrder( NeoStores neoStores, long nodeId, int... types )
    {
        NodeRecord node = neoStores.getNodeStore().getRecord( nodeId );
        assertTrue( "Node should be dense, is " + node, node.isDense() );
        long groupId = node.getNextRel();
        int cursor = 0;
        List<RelationshipGroupRecord> seen = new ArrayList<>();
        while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipGroupRecord group = neoStores.getRelationshipGroupStore().getRecord( groupId );
            seen.add( group );
            assertEquals( "Invalid type, seen groups so far " + seen, types[cursor++], group.getType() );
            groupId = group.getNext();
        }
        assertEquals( "Not enough relationship group records found in chain for " + node, types.length, cursor );
    }

    private Iterable<NodePropertyUpdate> indexUpdatesOf( NeoStores neoStores, TransactionRecordState state )
            throws IOException, TransactionFailureException
    {
        return indexUpdatesOf( neoStores, transactionRepresentationOf( state ) );
    }

    private Iterable<NodePropertyUpdate> indexUpdatesOf( NeoStores neoStores, TransactionRepresentation transaction )
            throws IOException
    {
        NodePropertyCommandsExtractor extractor = new NodePropertyCommandsExtractor();
        transaction.accept( extractor );

        OnlineIndexUpdates lazyIndexUpdates = new OnlineIndexUpdates( neoStores.getNodeStore(),
                neoStores.getPropertyStore(), new PropertyLoader( neoStores ) );
        lazyIndexUpdates.feed( extractor.propertyCommandsByNodeIds(), extractor.nodeCommandsById() );
        return lazyIndexUpdates;
    }

    private PhysicalTransactionRepresentation transactionRepresentationOf( TransactionRecordState writeTransaction )
            throws TransactionFailureException
    {
        List<Command> commands = new ArrayList<>();
        writeTransaction.extractCommands( commands );
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
        tx.setHeader( new byte[0], 0, 0, 0, 0, 0, 0 );
        return tx;
    }

    private void assertCommand( Command next, Class klass )
    {
        assertTrue( "Expected " + klass + ". was: " + next, klass.isInstance( next ) );
    }

    private CommittedTransactionRepresentation readFromChannel( ReadableVersionableLogChannel channel )
            throws IOException
    {
        LogEntryReader<ReadableVersionableLogChannel> logEntryReader = new VersionAwareLogEntryReader<>(
                new RecordStorageCommandReaderFactory() );
        try ( PhysicalTransactionCursor<ReadableVersionableLogChannel> cursor = new PhysicalTransactionCursor<>(
                channel, logEntryReader ) )
        {
            assertTrue( cursor.next() );
            return cursor.get();
        }
    }

    private void writeToChannel( TransactionRepresentation transaction, WritableLogChannel channel ) throws IOException
    {
        TransactionLogWriter writer = new TransactionLogWriter(
                new LogEntryWriter( channel, new CommandWriter( channel ) ) );
        writer.append( transaction, 2 );
    }

    private TransactionRecordState nodeWithDynamicLabelRecord( NeoStores store, AtomicLong nodeId,
            AtomicLong dynamicLabelRecordId )
    {
        NeoStoreTransactionContext context = newContext( store );
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
        NeoStoreTransactionContext context = newContext( store );
        TransactionRecordState recordState = recordState( store, context );
        recordState.nodeDelete( nodeId );
        return recordState;
    }

    private void apply( BatchTransactionApplier applier, TransactionRepresentation transaction ) throws Exception
    {
        CommandHandlerContract.apply( applier, new TransactionToApply( transaction ) );
    }

    private void apply( NeoStores neoStores, TransactionRepresentation transaction ) throws Exception
    {
        BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( neoStores, mock( CacheAccessBackDoor.class ),
                LockService.NO_LOCK_SERVICE );
        apply( applier, transaction );
    }

    private void apply( NeoStores neoStores, TransactionRecordState state ) throws Exception
    {
        BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( neoStores, mock( CacheAccessBackDoor.class ),
                LockService.NO_LOCK_SERVICE );
        apply( applier, transactionRepresentationOf( state ) );
    }

    private TransactionRecordState recordState( NeoStores store, NeoStoreTransactionContext context )
    {
        return new TransactionRecordState( store, new IntegrityValidator( store, mock( IndexingService.class ) ),
                context );
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

    private NeoStoreTransactionContext newContext( NeoStores neoStores )
    {
        return new NeoStoreTransactionContext( neoStores, mock( Locks.Client.class ) );
    }

    private void assertDynamicLabelRecordInUse( NeoStores store, long id, boolean inUse )
    {
        DynamicRecord record = store.getNodeStore().getDynamicLabelStore().forceGetRecord( id );
        assertTrue( inUse == record.inUse() );
    }

    private String string( int length )
    {
        StringBuilder result = new StringBuilder();
        char ch = 'a';
        for ( int i = 0; i < length; i++ )
        {
            result.append( (char) ((ch + (i % 10))) );
        }
        return result.toString();
    }

    private PropertyCommand singlePropertyCommand( Collection<Command> commands )
    {
        return (PropertyCommand) single( filter( t -> t instanceof PropertyCommand, commands ) );
    }
}
