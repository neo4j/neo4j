/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.impl.api.BatchTransactionApplier;
import org.neo4j.kernel.impl.api.CommandVisitor;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexingUpdateService;
import org.neo4j.kernel.impl.api.index.NodePropertyCommandsExtractor;
import org.neo4j.kernel.impl.api.index.NodeUpdates;
import org.neo4j.kernel.impl.api.index.PropertyPhysicalToLogicalConverter;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipGroupCommand;
import org.neo4j.kernel.impl.transaction.command.CommandHandlerContract;
import org.neo4j.kernel.impl.transaction.command.NeoStoreBatchTransactionApplier;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.InMemoryVersionableReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.test.rule.NeoStoresRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory.uniqueForLabel;
import static org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory.forLabel;
import static org.neo4j.kernel.impl.api.index.TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.store.record.ConstraintRule.constraintRule;
import static org.neo4j.kernel.impl.store.record.IndexRule.indexRule;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

public class TransactionRecordStateTest
{
    private static final String LONG_STRING = "string value long enough not to be stored as a short string";
    private static final int propertyId1 = 1;
    private static final int propertyId2 = 2;
    private static final Value value1 = Values.of( "first" );
    private static final Value value2 = Values.of( 4 );
    private static final long[] noLabels = new long[0];
    private final long[] oneLabelId = new long[]{3};
    private final long[] secondLabelId = new long[]{4};
    private final long[] bothLabelIds = new long[]{3, 4};

    public static void assertRelationshipGroupDoesNotExist( RecordChangeSet recordChangeSet, NodeRecord node,
            int type )
    {
        assertNull( getRelationshipGroup( recordChangeSet, node, type ) );
    }

    public static void assertDenseRelationshipCounts( RecordChangeSet recordChangeSet,
            long nodeId, int type, int outCount, int inCount )
    {
        RelationshipGroupRecord group = getRelationshipGroup( recordChangeSet,
                recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(), type ).forReadingData();
        assertNotNull( group );

        RelationshipRecord rel;
        long relId = group.getFirstOut();
        if ( relId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            rel = recordChangeSet.getRelRecords().getOrLoad( relId, null ).forReadingData();
            // count is stored in the back pointer of the first relationship in the chain
            assertEquals( "Stored relationship count for OUTGOING differs", outCount, rel.getFirstPrevRel() );
            assertEquals( "Manually counted relationships for OUTGOING differs", outCount,
                    manuallyCountRelationships( recordChangeSet, nodeId, relId ) );
        }

        relId = group.getFirstIn();
        if ( relId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            rel = recordChangeSet.getRelRecords().getOrLoad( relId, null ).forReadingData();
            assertEquals( "Stored relationship count for INCOMING differs", inCount, rel.getSecondPrevRel() );
            assertEquals( "Manually counted relationships for INCOMING differs", inCount,
                    manuallyCountRelationships( recordChangeSet, nodeId, relId ) );
        }
    }

    private static RecordProxy<RelationshipGroupRecord, Integer> getRelationshipGroup(
            RecordChangeSet recordChangeSet, NodeRecord node, int type )
    {
        long groupId = node.getNextRel();
        long previousGroupId = Record.NO_NEXT_RELATIONSHIP.intValue();
        while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RecordProxy<RelationshipGroupRecord, Integer> change =
                    recordChangeSet.getRelGroupRecords().getOrLoad( groupId, type );
            RelationshipGroupRecord record = change.forReadingData();
            record.setPrev( previousGroupId ); // not persistent so not a "change"
            if ( record.getType() == type )
            {
                return change;
            }
            previousGroupId = groupId;
            groupId = record.getNext();
        }
        return null;
    }

    private static int manuallyCountRelationships( RecordChangeSet recordChangeSet, long nodeId, long firstRelId )
    {
        int count = 0;
        long relId = firstRelId;
        while ( relId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            count++;
            RelationshipRecord record = recordChangeSet.getRelRecords().getOrLoad( relId, null ).forReadingData();
            relId = record.getFirstNode() == nodeId ? record.getFirstNextRel() : record.getSecondNextRel();
        }
        return count;
    }

    @Rule
    public final NeoStoresRule neoStoresRule = new NeoStoresRule( getClass() );
    private final IntegrityValidator integrityValidator = mock( IntegrityValidator.class );
    private RecordChangeSet recordChangeSet;

    @Test
    public void shouldCreateEqualNodePropertyUpdatesOnRecoveryOfCreatedNode() throws Exception
    {
        /* There was an issue where recovering a tx where a node with a label and a property
         * was created resulted in two exact copies of NodePropertyUpdates. */

        // GIVEN
        NeoStores neoStores = neoStoresRule.builder().build();
        long nodeId = 0;
        int labelId = 5;
        int propertyKeyId = 7;

        // -- an index
        long ruleId = 0;
        TransactionRecordState recordState = newTransactionRecordState( neoStores );
        SchemaRule rule = indexRule( ruleId, forLabel( labelId, propertyKeyId ), PROVIDER_DESCRIPTOR );
        recordState.createSchemaRule( rule );
        apply( neoStores, recordState );

        // -- and a tx creating a node with that label and property key
        recordState = newTransactionRecordState( neoStores );
        recordState.nodeCreate( nodeId );
        recordState.addLabelToNode( labelId, nodeId );
        recordState.nodeAddProperty( nodeId, propertyKeyId, Values.of( "Neo" ) );

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
        NeoStores neoStores = neoStoresRule.builder().build();
        TransactionRecordState recordState = newTransactionRecordState( neoStores );
        int nodeId = 0;
        recordState.nodeCreate( nodeId );
        int index = 0;
        recordState.nodeAddProperty( nodeId, index, string( 70 ) ); // will require a block of size 1
        apply( neoStores, recordState );

        // WHEN
        recordState = newTransactionRecordState( neoStores );
        int index2 = 1;
        recordState.nodeAddProperty( nodeId, index2, string( 40 ) ); // will require a block of size 4

        // THEN
        PhysicalTransactionRepresentation representation = transactionRepresentationOf( recordState );
        representation.accept( command -> ((Command)command).handle( new CommandVisitor.Adapter()
        {
            @Override
            public boolean visitPropertyCommand( PropertyCommand command )
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
        NeoStores neoStores = neoStoresRule.builder().build();
        long nodeId = 0;
        TransactionRecordState recordState = newTransactionRecordState( neoStores );
        Value value1 = Values.of( LONG_STRING );
        Value value2 = Values.of( LONG_STRING.getBytes() );
        recordState.nodeCreate( nodeId );
        recordState.nodeAddProperty( nodeId, propertyId1, value1 );
        recordState.nodeAddProperty( nodeId, propertyId2, value2 );
        apply( neoStores, recordState );

        // WHEN
        recordState = newTransactionRecordState( neoStores );
        addLabelsToNode( recordState, nodeId, oneLabelId );
        Iterable<NodeUpdates> indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        NodeUpdates expected = NodeUpdates.forNode( nodeId, noLabels, oneLabelId ).build();
        assertEquals( expected, Iterables.single( indexUpdates ) );
    }

    @Test
    public void shouldConvertMixedLabelAdditionAndSetPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        NeoStores neoStores = neoStoresRule.builder().build();
        long nodeId = 0;
        TransactionRecordState recordState = newTransactionRecordState( neoStores );
        recordState.nodeCreate( nodeId );
        recordState.nodeAddProperty( nodeId, propertyId1, value1 );
        addLabelsToNode( recordState, nodeId, oneLabelId );
        apply( neoStores, recordState );

        // WHEN
        recordState = newTransactionRecordState( neoStores );
        recordState.nodeAddProperty( nodeId, propertyId2, value2 );
        addLabelsToNode( recordState, nodeId, secondLabelId );
        Iterable<NodeUpdates> indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        NodeUpdates expected =
                NodeUpdates.forNode( nodeId, oneLabelId, bothLabelIds )
                        .added( propertyId2, value2 )
                        .build();
        assertEquals( expected, Iterables.single( indexUpdates ) );
    }

    @Test
    public void shouldConvertLabelRemovalToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        NeoStores neoStores = neoStoresRule.builder().build();
        long nodeId = 0;
        TransactionRecordState recordState = newTransactionRecordState( neoStores );
        recordState.nodeCreate( nodeId );
        recordState.nodeAddProperty( nodeId, propertyId1, value1 );
        recordState.nodeAddProperty( nodeId, propertyId2, value2 );
        addLabelsToNode( recordState, nodeId, oneLabelId );
        apply( neoStores, recordState );

        // WHEN
        recordState = newTransactionRecordState( neoStores );
        removeLabelsFromNode( recordState, nodeId, oneLabelId );
        Iterable<NodeUpdates> indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        NodeUpdates expected = NodeUpdates.forNode( nodeId, oneLabelId, noLabels ).build();
        assertEquals( expected, Iterables.single( indexUpdates ) );
    }

    @Test
    public void shouldConvertMixedLabelRemovalAndRemovePropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        NeoStores neoStores = neoStoresRule.builder().build();
        long nodeId = 0;
        TransactionRecordState recordState = newTransactionRecordState( neoStores );
        recordState.nodeCreate( nodeId );
        recordState.nodeAddProperty( nodeId, propertyId1, value1 );
        addLabelsToNode( recordState, nodeId, bothLabelIds );
        apply( neoStores, recordState );

        // WHEN
        recordState = newTransactionRecordState( neoStores );
        recordState.nodeRemoveProperty( nodeId, propertyId1 );
        removeLabelsFromNode( recordState, nodeId, secondLabelId );
        Iterable<NodeUpdates> indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        NodeUpdates expected =
                NodeUpdates.forNode( nodeId, bothLabelIds, oneLabelId )
                        .removed( propertyId1, value1 )
                        .build();
        assertEquals( expected, Iterables.single( indexUpdates ) );
    }

    @Test
    public void shouldConvertMixedLabelRemovalAndAddPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        NeoStores neoStores = neoStoresRule.builder().build();
        long nodeId = 0;
        TransactionRecordState recordState = newTransactionRecordState( neoStores );
        recordState.nodeCreate( nodeId );
        recordState.nodeAddProperty( nodeId, propertyId1, value1 );
        addLabelsToNode( recordState, nodeId, bothLabelIds );
        apply( neoStores, recordState );

        // WHEN
        recordState = newTransactionRecordState( neoStores );
        recordState.nodeAddProperty( nodeId, propertyId2, value2 );
        removeLabelsFromNode( recordState, nodeId, secondLabelId );
        Iterable<NodeUpdates> indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        NodeUpdates expected =
                NodeUpdates.forNode( nodeId, bothLabelIds, oneLabelId )
                        .added( propertyId2, value2 )
                        .build();
        assertEquals( expected, Iterables.single( indexUpdates ) );
    }

    @Test
    public void shouldConvertChangedPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        NeoStores neoStores = neoStoresRule.builder().build();
        int nodeId = 0;
        TransactionRecordState recordState = newTransactionRecordState( neoStores );
        recordState.nodeCreate( nodeId );
        recordState.nodeAddProperty( nodeId, propertyId1, value1 );
        recordState.nodeAddProperty( nodeId, propertyId2, value2 );
        apply( neoStores, transactionRepresentationOf( recordState ) );

        // WHEN
        Value newValue1 = Values.of( "new" );
        Value newValue2 = Values.of( "new 2" );
        recordState = newTransactionRecordState( neoStores );
        recordState.nodeChangeProperty( nodeId, propertyId1, newValue1 );
        recordState.nodeChangeProperty( nodeId, propertyId2, newValue2 );
        Iterable<NodeUpdates> indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        NodeUpdates expected =
                NodeUpdates.forNode( nodeId )
                        .changed( propertyId1, value1, newValue1 )
                        .changed( propertyId2, value2, newValue2 )
                        .build();
        assertEquals( expected, Iterables.single( indexUpdates ) );
    }

    @Test
    public void shouldConvertRemovedPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        NeoStores neoStores = neoStoresRule.builder().build();
        int nodeId = 0;
        TransactionRecordState recordState = newTransactionRecordState( neoStores );
        recordState.nodeCreate( nodeId );
        addLabelsToNode( recordState, nodeId, oneLabelId );
        recordState.nodeAddProperty( nodeId, propertyId1, value1 );
        recordState.nodeAddProperty( nodeId, propertyId2, value2 );
        apply( neoStores, transactionRepresentationOf( recordState ) );

        // WHEN
        recordState = newTransactionRecordState( neoStores );
        recordState.nodeRemoveProperty( nodeId, propertyId1 );
        recordState.nodeRemoveProperty( nodeId, propertyId2 );
        Iterable<NodeUpdates> indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        NodeUpdates expected =
                NodeUpdates.forNode( nodeId, oneLabelId )
                        .removed( propertyId1, value1 )
                        .removed( propertyId2, value2 )
                        .build();
        assertEquals( expected, Iterables.single( indexUpdates ) );
    }

    @Test
    public void shouldDeleteDynamicLabelsForDeletedNode() throws Throwable
    {
        // GIVEN a store that has got a node with a dynamic label record
        NeoStores store = neoStoresRule.builder().build();
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
        NeoStores store = neoStoresRule.builder().build();
        BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( store, mock( CacheAccessBackDoor.class ),
                LockService.NO_LOCK_SERVICE );
        AtomicLong nodeId = new AtomicLong();
        AtomicLong dynamicLabelRecordId = new AtomicLong();
        apply( applier, transaction( nodeWithDynamicLabelRecord( store, nodeId, dynamicLabelRecordId ) ) );
        assertDynamicLabelRecordInUse( store, dynamicLabelRecordId.get(), true );

        // WHEN applying a transaction, which has first round-tripped through a log (written then read)
        TransactionRepresentation transaction = transaction( deleteNode( store, nodeId.get() ) );
        InMemoryVersionableReadableClosablePositionAwareChannel channel = new InMemoryVersionableReadableClosablePositionAwareChannel();
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
        NeoStores neoStores = neoStoresRule.builder()
                .with( GraphDatabaseSettings.dense_node_threshold.name(), "1" ).build();
        TransactionRecordState recordState = newTransactionRecordState( neoStores );
        long nodeId = 0;
        long relId = 1;
        recordState.nodeCreate( nodeId );
        recordState.relCreate( relId++, 0, nodeId, nodeId );
        recordState.relCreate( relId, 0, nodeId, nodeId );
        recordState.nodeAddProperty( nodeId, 0, value2 );

        // WHEN
        Collection<StorageCommand> commands = new ArrayList<>();
        recordState.extractCommands( commands );

        // THEN
        Iterator<StorageCommand> commandIterator = commands.iterator();

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
        NeoStores neoStores = neoStoresRule.builder()
                .with( GraphDatabaseSettings.dense_node_threshold.name(), "1" ).build();
        TransactionRecordState recordState = newTransactionRecordState( neoStores );
        long nodeId = 0;
        long relId1 = 1;
        long relId2 = 2;
        long relId3 = 3;
        recordState.nodeCreate( nodeId );
        recordState.relCreate( relId1, 0, nodeId, nodeId );
        recordState.relCreate( relId2, 0, nodeId, nodeId );
        recordState.nodeAddProperty( nodeId, 0, Values.of( 101 ) );
        BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( neoStores, mock( CacheAccessBackDoor.class ),
                LockService.NO_LOCK_SERVICE );
        apply( applier, transaction( recordState ) );

        recordState = newTransactionRecordState( neoStores );
        recordState.nodeChangeProperty( nodeId, 0, Values.of( 102 ) );
        recordState.relCreate( relId3, 0, nodeId, nodeId );
        recordState.relAddProperty( relId1, 0, Values.of( 123 ) );

        // WHEN
        Collection<StorageCommand> commands = new ArrayList<>();
        recordState.extractCommands( commands );

        // THEN
        Iterator<StorageCommand> commandIterator = commands.iterator();

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
        // Given:
        // - dense node threshold of 5
        // - node with 4 rels of type relationshipB and 1 rel of type relationshipB
        NeoStores neoStore = neoStoresRule.builder()
                .with( GraphDatabaseSettings.dense_node_threshold.name(), "5" ).build();
        int relationshipA = 0;
        int relationshipB = 1;
        TransactionRecordState state = newTransactionRecordState( neoStore );
        state.nodeCreate( 0 );
        state.relCreate( 0, relationshipA, 0, 0 );
        state.relCreate( 1, relationshipA, 0, 0 );
        state.relCreate( 2, relationshipA, 0, 0 );
        state.relCreate( 3, relationshipA, 0, 0 );
        state.relCreate( 4, relationshipB, 0, 0 );
        apply( neoStore, state );

        // When doing a tx where a relationship of type A for the node is create and rel of type relationshipB is deleted
        state = newTransactionRecordState( neoStore );
        state.relCreate( 5, relationshipA, 0, 0 ); // here this node should be converted to dense and the groups should be created
        state.relDelete( 4 ); // here the group relationshipB should be delete

        // Then
        Collection<StorageCommand> commands = new ArrayList<>();
        state.extractCommands( commands );
        RelationshipGroupCommand group = singleRelationshipGroupCommand( commands );
        assertEquals( relationshipA, group.getAfter().getType() );
    }

    @Test
    public void shouldExtractDeleteCommandsInCorrectOrder() throws Exception
    {
        // GIVEN
        NeoStores neoStores = neoStoresRule.builder()
                .with( GraphDatabaseSettings.dense_node_threshold.name(), "1" ).build();
        TransactionRecordState recordState = newTransactionRecordState( neoStores );
        long nodeId1 = 0;
        long nodeId2 = 1;
        long relId1 = 1;
        long relId2 = 2;
        long relId4 = 10;
        recordState.nodeCreate( nodeId1 );
        recordState.nodeCreate( nodeId2 );
        recordState.relCreate( relId1, 0, nodeId1, nodeId1 );
        recordState.relCreate( relId2, 0, nodeId1, nodeId1 );
        recordState.relCreate( relId4, 1, nodeId1, nodeId1 );
        recordState.nodeAddProperty( nodeId1, 0, value1 );
        BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( neoStores, mock( CacheAccessBackDoor.class ),
                LockService.NO_LOCK_SERVICE );
        apply( applier, transaction( recordState ) );

        recordState = newTransactionRecordState( neoStores );
        recordState.relDelete( relId4 );
        recordState.nodeDelete( nodeId2 );
        recordState.nodeRemoveProperty( nodeId1, 0 );

        // WHEN
        Collection<StorageCommand> commands = new ArrayList<>();
        recordState.extractCommands( commands );

        // THEN
        Iterator<StorageCommand> commandIterator = commands.iterator();

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
        NeoStores neoStores = neoStoresRule.builder().build();
        TransactionRecordState recordState = newTransactionRecordState( neoStores );

        final long indexId = neoStores.getSchemaStore().nextId();
        final long constraintId = neoStores.getSchemaStore().nextId();

        recordState.createSchemaRule( constraintRule( constraintId, uniqueForLabel( 1, 1 ), indexId ) );

        // WHEN
        recordState.extractCommands( new ArrayList<>() );

        // THEN
        verify( integrityValidator ).validateSchemaRule( any() );
    }

    @Test
    public void shouldCreateProperBeforeAndAfterPropertyCommandsWhenAddingProperty() throws Exception
    {
        // GIVEN
        NeoStores neoStores = neoStoresRule.builder().build();
        TransactionRecordState recordState = newTransactionRecordState( neoStores );

        int nodeId = 1;
        recordState.nodeCreate( nodeId );

        // WHEN
        recordState.nodeAddProperty( nodeId, propertyId1, value1 );
        Collection<StorageCommand> commands = new ArrayList<>();
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
        NeoStores neoStores = neoStoresRule.builder().build();
        long nodeId = 0;
        TransactionRecordState recordState = newTransactionRecordState( neoStores );

        // WHEN
        recordState.nodeCreate( nodeId );
        addLabelsToNode( recordState, nodeId, oneLabelId );
        recordState.nodeAddProperty( nodeId, propertyId1, value1 );
        recordState.nodeAddProperty( nodeId, propertyId2, value2 );
        Iterable<NodeUpdates> updates = indexUpdatesOf( neoStores, recordState );

        // THEN
        NodeUpdates expected =
                NodeUpdates.forNode( nodeId, noLabels, oneLabelId )
                        .added( propertyId1, value1 )
                        .added( propertyId2, value2 ).build();
        assertEquals( expected, Iterables.single( updates ) );
    }

    @Test
    public void shouldLockUpdatedNodes() throws Exception
    {
        // given
        LockService locks = mock( LockService.class, new Answer<Object>()
        {
            @Override
            public synchronized Object answer( final InvocationOnMock invocation )
            {
                // This is necessary because finalize() will also be called
                String name = invocation.getMethod().getName();
                if ( name.equals( "acquireNodeLock" ) || name.equals( "acquireRelationshipLock" ) )
                {
                    return mock( Lock.class, invocationOnMock -> null );
                }
                return null;
            }
        } );
        NeoStores neoStores = neoStoresRule.builder().build();
        NodeStore nodeStore = neoStores.getNodeStore();
        long[] nodes = { // allocate ids
                nodeStore.nextId(), nodeStore.nextId(), nodeStore.nextId(), nodeStore.nextId(), nodeStore.nextId(),
                nodeStore.nextId(), nodeStore.nextId(),};

        {
            // create the node records that we will modify in our main tx.
            TransactionRecordState tx = newTransactionRecordState( neoStores );
            for ( int i = 1; i < nodes.length - 1; i++ )
            {
                tx.nodeCreate( nodes[i] );
            }
            tx.nodeAddProperty( nodes[3], 0, Values.of( "old" ) );
            tx.nodeAddProperty( nodes[4], 0, Values.of( "old" ) );
            BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( neoStores, mock( CacheAccessBackDoor.class ),
                    locks );
            apply( applier, transaction( tx ) );
        }
        reset( locks );

        // These are the changes we want to assert locking on
        TransactionRecordState tx = newTransactionRecordState( neoStores );
        tx.nodeCreate( nodes[0] );
        tx.addLabelToNode( 0, nodes[1] );
        tx.nodeAddProperty( nodes[2], 0, Values.of( "value" ) );
        tx.nodeChangeProperty( nodes[3], 0, Values.of( "value" ) );
        tx.nodeRemoveProperty( nodes[4], 0 );
        tx.nodeDelete( nodes[5] );

        tx.nodeCreate( nodes[6] );
        tx.addLabelToNode( 0, nodes[6] );
        tx.nodeAddProperty( nodes[6], 0, Values.of( "value" ) );

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
        NeoStores neoStores = neoStoresRule.builder()
                .with( GraphDatabaseSettings.dense_node_threshold.name(), "10" ).build();
        TransactionRecordState tx = newTransactionRecordState( neoStores );
        long nodeId = neoStores.getNodeStore().nextId();

        tx.nodeCreate( nodeId );

        int typeA = (int) neoStores.getRelationshipTypeTokenStore().nextId();
        tx.createRelationshipTypeToken( "A", typeA );
        createRelationships( neoStores, tx, nodeId, typeA, INCOMING, 20 );

        BatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( neoStores, mock( CacheAccessBackDoor.class ),
                LockService.NO_LOCK_SERVICE );
        apply( applier, transaction( tx ) );

        tx = newTransactionRecordState( neoStores );

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

        ptx.accept( command -> ((Command)command).handle( new CommandVisitor.Adapter()
        {
            @Override
            public boolean visitRelationshipGroupCommand( Command.RelationshipGroupCommand command )
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
    public void shouldConvertToDenseNodeRepresentationWhenHittingThresholdWithDifferentTypes() throws Exception
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        NeoStores neoStores = neoStoresRule.builder()
                .with( GraphDatabaseSettings.dense_node_threshold.name(), "50" ).build();
        TransactionRecordState tx = newTransactionRecordState( neoStores );
        long nodeId = neoStores.getNodeStore().nextId();
        int typeA = 0;
        int typeB = 1;
        int typeC = 2;
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
        assertFalse( recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData().isDense() );

        // WHEN creating the relationship that pushes us over the threshold
        createRelationships( neoStores, tx, nodeId, typeC, INCOMING, 1 );

        // THEN the node should have been converted into a dense node
        assertTrue( recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData().isDense() );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeA, 6, 7 );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeB, 8, 9 );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeC, 10, 11 );
    }

    @Test
    public void shouldConvertToDenseNodeRepresentationWhenHittingThresholdWithTheSameTypeDifferentDirection()
            throws Exception
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        NeoStores neoStores = neoStoresRule.builder()
                .with( GraphDatabaseSettings.dense_node_threshold.name(), "49" ).build();
        TransactionRecordState tx = newTransactionRecordState( neoStores );
        long nodeId = neoStores.getNodeStore().nextId();
        int typeA = 0;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( "A", typeA );
        createRelationships( neoStores, tx, nodeId, typeA, OUTGOING, 24 );
        createRelationships( neoStores, tx, nodeId, typeA, INCOMING, 25 );

        // here we're at the edge
        assertFalse( recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData().isDense() );

        // WHEN creating the relationship that pushes us over the threshold
        createRelationships( neoStores, tx, nodeId, typeA, INCOMING, 1 );

        // THEN the node should have been converted into a dense node
        assertTrue( recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData().isDense() );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeA, 24, 26 );
    }

    @Test
    public void shouldConvertToDenseNodeRepresentationWhenHittingThresholdWithTheSameTypeSameDirection()
            throws Exception
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        NeoStores neoStores = neoStoresRule.builder()
                .with( GraphDatabaseSettings.dense_node_threshold.name(), "8" ).build();
        TransactionRecordState tx = newTransactionRecordState( neoStores );
        long nodeId = neoStores.getNodeStore().nextId();
        int typeA = 0;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( "A", typeA );
        createRelationships( neoStores, tx, nodeId, typeA, OUTGOING, 8 );

        // here we're at the edge
        assertFalse( recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData().isDense() );

        // WHEN creating the relationship that pushes us over the threshold
        createRelationships( neoStores, tx, nodeId, typeA, OUTGOING, 1 );

        // THEN the node should have been converted into a dense node
        assertTrue( recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData().isDense() );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeA, 9, 0 );
    }

    @Test
    public void shouldMaintainCorrectDataWhenDeletingFromDenseNodeWithOneType() throws Exception
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        NeoStores neoStores = neoStoresRule.builder()
                .with( GraphDatabaseSettings.dense_node_threshold.name(), "13" ).build();
        TransactionRecordState tx = newTransactionRecordState( neoStores );
        int nodeId = (int) neoStores.getNodeStore().nextId();
        int typeA = 0;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( "A", typeA );
        long[] relationshipsCreated = createRelationships( neoStores, tx, nodeId, typeA, INCOMING, 15 );

        //WHEN
        tx.relDelete( relationshipsCreated[0] );

        // THEN the node should have been converted into a dense node
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeA, 0, 14 );
    }

    @Test
    public void shouldMaintainCorrectDataWhenDeletingFromDenseNodeWithManyTypes() throws Exception
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        NeoStores neoStores = neoStoresRule.builder()
                .with( GraphDatabaseSettings.dense_node_threshold.name(), "1" ).build();
        TransactionRecordState tx = newTransactionRecordState( neoStores );
        long nodeId = neoStores.getNodeStore().nextId();
        int typeA = 0;
        int typeB = 12;
        int typeC = 600;
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
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeA, 1, 0 );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeB, 1, 1 );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeC, 1, 1 );

        // WHEN
        tx.relDelete( relationshipsCreatedAOutgoing[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeA );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeB, 1, 1 );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeC, 1, 1 );

        // WHEN
        tx.relDelete( relationshipsCreatedBIncoming[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeA );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeB, 1, 0 );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeC, 1, 1 );

        // WHEN
        tx.relDelete( relationshipsCreatedBOutgoing[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeA );
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeB );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeC, 1, 1 );

        // WHEN
        tx.relDelete( relationshipsCreatedCIncoming[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeA );
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeB );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeC, 1, 0 );

        // WHEN
        tx.relDelete( relationshipsCreatedCOutgoing[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeA );
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeB );
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(),
                typeC );
    }

    @Test
    public void shouldSortRelationshipGroups() throws Throwable
    {
        // GIVEN
        int type5 = 5;
        int type10 = 10;
        int type15 = 15;
        NeoStores neoStores = neoStoresRule.builder()
                .with( GraphDatabaseSettings.dense_node_threshold.name(), "1" ).build();
        {
            TransactionRecordState recordState = newTransactionRecordState( neoStores );
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
            TransactionRecordState recordState = newTransactionRecordState( neoStores );
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
            TransactionRecordState recordState = newTransactionRecordState( neoStores );
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
            TransactionRecordState recordState = newTransactionRecordState( neoStores );
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

    @Test
    public void shouldPrepareRelevantRecords() throws Exception
    {
        // GIVEN
        PrepareTrackingRecordFormats format = new PrepareTrackingRecordFormats( Standard.LATEST_RECORD_FORMATS );
        NeoStores neoStores = neoStoresRule.builder()
                .with( format )
                .with( GraphDatabaseSettings.dense_node_threshold.name(), "1" ).build();

        // WHEN
        TransactionRecordState state = newTransactionRecordState( neoStores );
        state.nodeCreate( 0 );
        state.relCreate( 0, 0, 0, 0 );
        state.relCreate( 1, 0, 0, 0 );
        state.relCreate( 2, 0, 0, 0 );
        List<StorageCommand> commands = new ArrayList<>();
        state.extractCommands( commands );

        // THEN
        int nodes = 0;
        int rels = 0;
        int groups = 0;
        for ( StorageCommand command : commands )
        {
            if ( command instanceof NodeCommand )
            {
                assertTrue( format.node().prepared( ((NodeCommand) command).getAfter() ) );
                nodes++;
            }
            else if ( command instanceof RelationshipCommand )
            {
                assertTrue( format.relationship().prepared( ((RelationshipCommand) command).getAfter() ) );
                rels++;
            }
            else if ( command instanceof RelationshipGroupCommand )
            {
                assertTrue( format.relationshipGroup().prepared( ((RelationshipGroupCommand) command).getAfter() ) );
                groups++;
            }
        }
        assertEquals( 1, nodes );
        assertEquals( 3, rels );
        assertEquals( 1, groups );
    }

    private void addLabelsToNode( TransactionRecordState recordState, long nodeId, long[] labelIds )
    {
        for ( long labelId : labelIds )
        {
            recordState.addLabelToNode( (int)labelId, nodeId );
        }
    }

    private void removeLabelsFromNode( TransactionRecordState recordState, long nodeId, long[] labelIds )
    {
        for ( long labelId : labelIds )
        {
            recordState.removeLabelFromNode( (int)labelId, nodeId );
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
        NodeStore nodeStore = neoStores.getNodeStore();
        NodeRecord node = nodeStore.getRecord( nodeId, nodeStore.newRecord(), NORMAL );
        assertTrue( "Node should be dense, is " + node, node.isDense() );
        long groupId = node.getNextRel();
        int cursor = 0;
        List<RelationshipGroupRecord> seen = new ArrayList<>();
        while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RecordStore<RelationshipGroupRecord> relationshipGroupStore = neoStores.getRelationshipGroupStore();
            RelationshipGroupRecord group = relationshipGroupStore.getRecord( groupId,
                    relationshipGroupStore.newRecord(), NORMAL );
            seen.add( group );
            assertEquals( "Invalid type, seen groups so far " + seen, types[cursor++], group.getType() );
            groupId = group.getNext();
        }
        assertEquals( "Not enough relationship group records found in chain for " + node, types.length, cursor );
    }

    private Iterable<NodeUpdates> indexUpdatesOf( NeoStores neoStores, TransactionRecordState state )
            throws IOException, TransactionFailureException
    {
        return indexUpdatesOf( neoStores, transactionRepresentationOf( state ) );
    }

    private Iterable<NodeUpdates> indexUpdatesOf( NeoStores neoStores, TransactionRepresentation transaction )
            throws IOException
    {
        NodePropertyCommandsExtractor extractor = new NodePropertyCommandsExtractor();
        transaction.accept( extractor );

        CollectingIndexingUpdateService indexingUpdateService = new CollectingIndexingUpdateService();
        OnlineIndexUpdates onlineIndexUpdates = new OnlineIndexUpdates( neoStores.getNodeStore(),
                indexingUpdateService,
                new PropertyPhysicalToLogicalConverter( neoStores.getPropertyStore() ) );
        onlineIndexUpdates.feed( extractor.propertyCommandsByNodeIds(), extractor.nodeCommandsById() );
        return indexingUpdateService.nodeUpdatesList;
    }

    private PhysicalTransactionRepresentation transactionRepresentationOf( TransactionRecordState writeTransaction )
            throws TransactionFailureException
    {
        List<StorageCommand> commands = new ArrayList<>();
        writeTransaction.extractCommands( commands );
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
        tx.setHeader( new byte[0], 0, 0, 0, 0, 0, 0 );
        return tx;
    }

    private void assertCommand( StorageCommand next, Class<?> klass )
    {
        assertTrue( "Expected " + klass + ". was: " + next, klass.isInstance( next ) );
    }

    private CommittedTransactionRepresentation readFromChannel( ReadableLogChannel channel )
            throws IOException
    {
        LogEntryReader<ReadableLogChannel> logEntryReader = new VersionAwareLogEntryReader<>();
        try ( PhysicalTransactionCursor<ReadableLogChannel> cursor = new PhysicalTransactionCursor<>(
                channel, logEntryReader ) )
        {
            assertTrue( cursor.next() );
            return cursor.get();
        }
    }

    private void writeToChannel( TransactionRepresentation transaction, FlushableChannel channel ) throws IOException
    {
        TransactionLogWriter writer = new TransactionLogWriter( new LogEntryWriter( channel ) );
        writer.append( transaction, 2 );
    }

    private TransactionRecordState nodeWithDynamicLabelRecord( NeoStores store, AtomicLong nodeId,
            AtomicLong dynamicLabelRecordId )
    {
        TransactionRecordState recordState = newTransactionRecordState( store );

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
        NodeRecord node = Iterables.single( recordChangeSet.getNodeRecords().changes() ).forReadingData();
        dynamicLabelRecordId.set( Iterables.single( node.getDynamicLabelRecords() ).getId() );

        return recordState;
    }

    private TransactionRecordState deleteNode( NeoStores store, long nodeId )
    {
        TransactionRecordState recordState = newTransactionRecordState( store );
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

    private TransactionRecordState newTransactionRecordState( NeoStores neoStores )
    {
        Loaders loaders = new Loaders( neoStores );
        recordChangeSet = new RecordChangeSet( loaders );
        PropertyTraverser propertyTraverser = new PropertyTraverser();
        RelationshipGroupGetter relationshipGroupGetter =
                new RelationshipGroupGetter( neoStores.getRelationshipGroupStore() );
        PropertyDeleter propertyDeleter = new PropertyDeleter( propertyTraverser );
        return new TransactionRecordState( neoStores, integrityValidator, recordChangeSet, 0,
                new NoOpClient(),
                new RelationshipCreator( relationshipGroupGetter,
                        neoStores.getRelationshipGroupStore().getStoreHeaderInt() ),
                new RelationshipDeleter( relationshipGroupGetter, propertyDeleter ),
                new PropertyCreator( neoStores.getPropertyStore(), propertyTraverser ),
                propertyDeleter );
    }

    private TransactionRepresentation transaction( TransactionRecordState recordState )
            throws TransactionFailureException
    {
        List<StorageCommand> commands = new ArrayList<>();
        recordState.extractCommands( commands );
        PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( commands );
        transaction.setHeader( new byte[0], 0, 0, 0, 0, 0, 0 );
        return transaction;
    }

    private void assertDynamicLabelRecordInUse( NeoStores store, long id, boolean inUse )
    {
        DynamicArrayStore dynamicLabelStore = store.getNodeStore().getDynamicLabelStore();
        DynamicRecord record = dynamicLabelStore.getRecord( id, dynamicLabelStore.nextRecord(), FORCE );
        assertTrue( inUse == record.inUse() );
    }

    private Value string( int length )
    {
        StringBuilder result = new StringBuilder();
        char ch = 'a';
        for ( int i = 0; i < length; i++ )
        {
            result.append( (char) ((ch + (i % 10))) );
        }
        return Values.of( result.toString() );
    }

    private PropertyCommand singlePropertyCommand( Collection<StorageCommand> commands )
    {
        return (PropertyCommand) Iterables.single( filter( t -> t instanceof PropertyCommand, commands ) );
    }

    private RelationshipGroupCommand singleRelationshipGroupCommand( Collection<StorageCommand> commands )
    {
        return (RelationshipGroupCommand) Iterables.single( filter( t -> t instanceof RelationshipGroupCommand, commands ) );
    }

    private class CollectingIndexingUpdateService implements IndexingUpdateService
    {
        final List<NodeUpdates> nodeUpdatesList = new ArrayList<>();

        @Override
        public void apply( IndexUpdates updates )
        {
        }

        @Override
        public Iterable<IndexEntryUpdate<SchemaDescriptor>> convertToIndexUpdates( NodeUpdates nodeUpdates )
        {
            nodeUpdatesList.add( nodeUpdates );
            return Iterables.empty();
        }
    }
}
