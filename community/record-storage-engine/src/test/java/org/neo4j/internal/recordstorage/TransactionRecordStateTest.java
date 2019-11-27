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
package org.neo4j.internal.recordstorage;

import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.PropertyCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipGroupCommand;
import org.neo4j.internal.recordstorage.Command.SchemaRuleCommand;
import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.log.InMemoryVersionableReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.lock.Lock;
import org.neo4j.lock.LockService;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.util.concurrent.WorkSync;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.collection.Iterables.asSet;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterables.filter;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.helpers.collection.Iterators.array;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptor.forRelType;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.uniqueForLabel;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;
import static org.neo4j.storageengine.api.IndexEntryUpdate.change;
import static org.neo4j.storageengine.api.IndexEntryUpdate.remove;
import static org.neo4j.storageengine.api.TransactionApplicationMode.INTERNAL;

@EphemeralNeo4jLayoutExtension
@EphemeralPageCacheExtension
class TransactionRecordStateTest
{
    private static final String LONG_STRING = "string value long enough not to be stored as a short string";
    private static final int propertyId1 = 1;
    private static final int propertyId2 = 2;
    private static final Value value1 = Values.of( "first" );
    private static final Value value2 = Values.of( 4 );
    private static final int labelIdOne = 3;
    private static final int labelIdSecond = 4;
    private final long[] oneLabelId = new long[]{labelIdOne};
    private final long[] secondLabelId = new long[]{labelIdSecond};
    private final long[] bothLabelIds = new long[]{labelIdOne, labelIdSecond};
    private final IntegrityValidator integrityValidator = mock( IntegrityValidator.class );
    private RecordChangeSet recordChangeSet;
    private final SchemaCache schemaCache = new SchemaCache( new StandardConstraintRuleAccessor(), index -> index );
    private long nextRuleId = 1;

    @Inject
    private PageCache pageCache;
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private DatabaseLayout databaseLayout;

    private NeoStores neoStores;
    private IdGeneratorFactory idGeneratorFactory;

    @AfterEach
    void after()
    {
        neoStores.close();
    }

    private NeoStores createStores()
    {
        return createStores( Config.defaults() );
    }

    private NeoStores createStores( Config config )
    {
        return createStores( config, RecordFormatSelector.selectForConfig( config, NullLogProvider.getInstance() ) );
    }

    private NeoStores createStores( Config config, RecordFormats formats )
    {
        idGeneratorFactory = new DefaultIdGeneratorFactory( fs, immediate() );
        var storeFactory = new StoreFactory( databaseLayout, config, idGeneratorFactory, pageCache, fs,
                formats, NullLogProvider.getInstance() );
        return storeFactory.openAllNeoStores( true );
    }

    private static void assertRelationshipGroupDoesNotExist( RecordChangeSet recordChangeSet, NodeRecord node, int type )
    {
        assertNull( getRelationshipGroup( recordChangeSet, node, type ) );
    }

    private static void assertDenseRelationshipCounts( RecordChangeSet recordChangeSet, long nodeId, int type, int outCount, int inCount )
    {
        RecordProxy<RelationshipGroupRecord,Integer> proxy =
                getRelationshipGroup( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(), type );
        assertNotNull( proxy );
        RelationshipGroupRecord group = proxy.forReadingData();
        assertNotNull( group );

        RelationshipRecord rel;
        long relId = group.getFirstOut();
        if ( relId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            rel = recordChangeSet.getRelRecords().getOrLoad( relId, null ).forReadingData();
            // count is stored in the back pointer of the first relationship in the chain
            assertEquals( outCount, rel.getFirstPrevRel(), "Stored relationship count for OUTGOING differs" );
            assertEquals( outCount, manuallyCountRelationships( recordChangeSet, nodeId, relId ),
                "Manually counted relationships for OUTGOING differs" );
        }

        relId = group.getFirstIn();
        if ( relId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            rel = recordChangeSet.getRelRecords().getOrLoad( relId, null ).forReadingData();
            assertEquals( inCount, rel.getSecondPrevRel(), "Stored relationship count for INCOMING differs" );
            assertEquals( inCount, manuallyCountRelationships( recordChangeSet, nodeId, relId ),
                "Manually counted relationships for INCOMING differs" );
        }
    }

    private static RecordProxy<RelationshipGroupRecord,Integer> getRelationshipGroup( RecordChangeSet recordChangeSet, NodeRecord node, int type )
    {
        long groupId = node.getNextRel();
        long previousGroupId = Record.NO_NEXT_RELATIONSHIP.intValue();
        while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RecordProxy<RelationshipGroupRecord,Integer> change = recordChangeSet.getRelGroupRecords().getOrLoad( groupId, type );
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

    @Test
    void shouldCreateEqualEntityPropertyUpdatesOnRecoveryOfCreatedEntities() throws Exception
    {
        neoStores = createStores();
        /* There was an issue where recovering a tx where a node with a label and a property
         * was created resulted in two exact copies of NodePropertyUpdates. */

        // GIVEN
        long nodeId = 0;
        long relId = 1;
        int labelId = 5;
        int relTypeId = 4;
        int propertyKeyId = 7;

        // -- indexes
        long nodeRuleId = 0;
        TransactionRecordState recordState = newTransactionRecordState();
        SchemaRule nodeRule = IndexPrototype.forSchema( forLabel( labelId, propertyKeyId ) ).withName( "index_" + nodeRuleId ).materialise( nodeRuleId );
        recordState.schemaRuleCreate( nodeRuleId, false, nodeRule );
        long relRuleId = 1;
        SchemaRule relRule = IndexPrototype.forSchema( forRelType( relTypeId, propertyKeyId ) ).withName( "index_" + relRuleId ).materialise( relRuleId );
        recordState.schemaRuleCreate( relRuleId, false, relRule );
        apply( recordState );

        // -- and a tx creating a node and a rel for those indexes
        recordState = newTransactionRecordState();
        recordState.nodeCreate( nodeId );
        recordState.addLabelToNode( labelId, nodeId );
        recordState.nodeAddProperty( nodeId, propertyKeyId, Values.of( "Neo" ) );
        recordState.relCreate( relId, relTypeId, nodeId, nodeId );
        recordState.relAddProperty( relId, propertyKeyId, Values.of( "Oen" ) );

        // WHEN
        CommandsToApply transaction = transaction( recordState );
        PropertyCommandsExtractor extractor = new PropertyCommandsExtractor();
        transaction.accept( extractor );

        // THEN
        // -- later recovering that tx, there should be only one update for each type
        assertTrue( extractor.containsAnyEntityOrPropertyUpdate() );
        MutableLongSet recoveredNodeIds = new LongHashSet();
        recoveredNodeIds.addAll( entityIds( extractor.getNodeCommands() ) );
        assertEquals( 1, recoveredNodeIds.size() );
        assertEquals( nodeId, recoveredNodeIds.longIterator().next() );

        MutableLongSet recoveredRelIds = new LongHashSet();
        recoveredRelIds.addAll( entityIds( extractor.getRelationshipCommands() ) );
        assertEquals( 1, recoveredRelIds.size() );
        assertEquals( relId, recoveredRelIds.longIterator().next() );
    }

    @Test
    void shouldWriteProperPropertyRecordsWhenOnlyChangingLinkage() throws Exception
    {
        neoStores = createStores();
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
        TransactionRecordState recordState = newTransactionRecordState();
        int nodeId = 0;
        recordState.nodeCreate( nodeId );
        int index = 0;
        recordState.nodeAddProperty( nodeId, index, string( 70 ) ); // will require a block of size 1
        apply( recordState );

        // WHEN
        recordState = newTransactionRecordState();
        int index2 = 1;
        recordState.nodeAddProperty( nodeId, index2, string( 40 ) ); // will require a block of size 4

        // THEN
        CommandsToApply representation = transaction( recordState );
        representation.accept( command -> ((Command) command).handle( new CommandVisitor.Adapter()
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
    void shouldConvertLabelAdditionToNodePropertyUpdates() throws Exception
    {
        neoStores = createStores();
        // GIVEN
        long nodeId = 0;
        TransactionRecordState recordState = newTransactionRecordState();
        Value value1 = Values.of( LONG_STRING );
        Value value2 = Values.of( LONG_STRING.getBytes() );
        recordState.nodeCreate( nodeId );
        recordState.nodeAddProperty( nodeId, propertyId1, value1 );
        recordState.nodeAddProperty( nodeId, propertyId2, value2 );
        apply( recordState );
        IndexDescriptor rule1 = createIndex( labelIdOne, propertyId1 );
        IndexDescriptor rule2 = createIndex( labelIdOne, propertyId2 );

        // WHEN
        recordState = newTransactionRecordState();
        addLabelsToNode( recordState, nodeId, oneLabelId );
        var indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        assertEquals( asSet(
                add( nodeId, rule1, value1 ),
                add( nodeId, rule2, value2 ) ),
                asSet( single( indexUpdates ) ) );
    }

    @Test
    void shouldConvertMixedLabelAdditionAndSetPropertyToNodePropertyUpdates() throws Exception
    {
        neoStores = createStores();
        // GIVEN
        long nodeId = 0;
        TransactionRecordState recordState = newTransactionRecordState();
        recordState.nodeCreate( nodeId );
        recordState.nodeAddProperty( nodeId, propertyId1, value1 );
        addLabelsToNode( recordState, nodeId, oneLabelId );
        apply( recordState );
        IndexDescriptor rule1 = createIndex( labelIdOne, propertyId2 );
        IndexDescriptor rule2 = createIndex( labelIdOne, propertyId1, propertyId2 );

        // WHEN
        recordState = newTransactionRecordState();
        recordState.nodeAddProperty( nodeId, propertyId2, value2 );
        addLabelsToNode( recordState, nodeId, secondLabelId );
        var indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        assertEquals( asSet(
                add( nodeId, rule1, value2 ),
                add( nodeId, rule2, value1, value2 ) ),
                asSet( single( indexUpdates ) ) );
    }

    @Test
    void shouldConvertLabelRemovalToNodePropertyUpdates() throws Exception
    {
        neoStores = createStores();
        // GIVEN
        long nodeId = 0;
        TransactionRecordState recordState = newTransactionRecordState();
        recordState.nodeCreate( nodeId );
        recordState.nodeAddProperty( nodeId, propertyId1, value1 );
        recordState.nodeAddProperty( nodeId, propertyId2, value2 );
        addLabelsToNode( recordState, nodeId, oneLabelId );
        apply( recordState );
        IndexDescriptor rule = createIndex( labelIdOne, propertyId1 );

        // WHEN
        recordState = newTransactionRecordState();
        removeLabelsFromNode( recordState, nodeId, oneLabelId );
        var indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        assertEquals( asSet( remove( nodeId, rule, value1 ) ), asSet( single( indexUpdates ) ) );
    }

    @Test
    void shouldConvertMixedLabelRemovalAndRemovePropertyToNodePropertyUpdates() throws Exception
    {
        neoStores = createStores();
        // GIVEN
        long nodeId = 0;
        TransactionRecordState recordState = newTransactionRecordState();
        recordState.nodeCreate( nodeId );
        recordState.nodeAddProperty( nodeId, propertyId1, value1 );
        addLabelsToNode( recordState, nodeId, bothLabelIds );
        apply( recordState );
        IndexDescriptor rule1 = createIndex( labelIdOne, propertyId1 );
        IndexDescriptor rule2 = createIndex( labelIdSecond, propertyId1 );

        // WHEN
        recordState = newTransactionRecordState();
        recordState.nodeRemoveProperty( nodeId, propertyId1 );
        removeLabelsFromNode( recordState, nodeId, secondLabelId );
        var indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        assertEquals( asSet(
                remove( nodeId, rule1, value1 ),
                remove( nodeId, rule2, value1 ) ),
                asSet( single( indexUpdates ) ) );
    }

    @Test
    void shouldConvertMixedLabelRemovalAndAddPropertyToNodePropertyUpdates() throws Exception
    {
        neoStores = createStores();
        // GIVEN
        long nodeId = 0;
        TransactionRecordState recordState = newTransactionRecordState();
        recordState.nodeCreate( nodeId );
        recordState.nodeAddProperty( nodeId, propertyId1, value1 );
        addLabelsToNode( recordState, nodeId, bothLabelIds );
        apply( recordState );
        IndexDescriptor rule2 = createIndex( labelIdOne, propertyId2 );
        IndexDescriptor rule3 = createIndex( labelIdSecond, propertyId1 );

        // WHEN
        recordState = newTransactionRecordState();
        recordState.nodeAddProperty( nodeId, propertyId2, value2 );
        removeLabelsFromNode( recordState, nodeId, secondLabelId );
        var indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        assertEquals( asSet(
                add( nodeId, rule2, value2 ),
                remove( nodeId, rule3, value1 ) ),
                asSet( single( indexUpdates ) ) );
    }

    @Test
    void shouldConvertChangedPropertyToNodePropertyUpdates() throws Exception
    {
        neoStores = createStores();
        // GIVEN
        int nodeId = 0;
        TransactionRecordState recordState = newTransactionRecordState();
        recordState.nodeCreate( nodeId );
        recordState.nodeAddProperty( nodeId, propertyId1, value1 );
        recordState.nodeAddProperty( nodeId, propertyId2, value2 );
        addLabelsToNode( recordState, nodeId, oneLabelId );
        apply( transaction( recordState ) );
        IndexDescriptor rule1 = createIndex( labelIdOne, propertyId1 );
        IndexDescriptor rule2 = createIndex( labelIdOne, propertyId2 );
        IndexDescriptor rule3 = createIndex( labelIdOne, propertyId1, propertyId2 );

        // WHEN
        Value newValue1 = Values.of( "new" );
        Value newValue2 = Values.of( "new 2" );
        recordState = newTransactionRecordState();
        recordState.nodeChangeProperty( nodeId, propertyId1, newValue1 );
        recordState.nodeChangeProperty( nodeId, propertyId2, newValue2 );
        var indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        assertEquals( asSet(
                change( nodeId, rule1, value1, newValue1 ),
                change( nodeId, rule2, value2, newValue2 ),
                change( nodeId, rule3, array( value1, value2 ), array( newValue1, newValue2 ) ) ),
                asSet( single( indexUpdates ) ) );
    }

    @Test
    void shouldConvertRemovedPropertyToNodePropertyUpdates() throws Exception
    {
        neoStores = createStores();
        // GIVEN
        int nodeId = 0;
        TransactionRecordState recordState = newTransactionRecordState();
        recordState.nodeCreate( nodeId );
        addLabelsToNode( recordState, nodeId, oneLabelId );
        recordState.nodeAddProperty( nodeId, propertyId1, value1 );
        recordState.nodeAddProperty( nodeId, propertyId2, value2 );
        apply( transaction( recordState ) );
        IndexDescriptor rule1 = createIndex( labelIdOne, propertyId1 );
        IndexDescriptor rule2 = createIndex( labelIdOne, propertyId2 );
        IndexDescriptor rule3 = createIndex( labelIdOne, propertyId1, propertyId2 );

        // WHEN
        recordState = newTransactionRecordState();
        recordState.nodeRemoveProperty( nodeId, propertyId1 );
        recordState.nodeRemoveProperty( nodeId, propertyId2 );
        var indexUpdates = indexUpdatesOf( neoStores, recordState );

        // THEN
        assertEquals( asSet(
                remove( nodeId, rule1, value1 ),
                remove( nodeId, rule2, value2 ),
                remove( nodeId, rule3, value1, value2 ) ),
                asSet( single( indexUpdates ) ) );
    }

    @Test
    void shouldDeleteDynamicLabelsForDeletedNode() throws Throwable
    {
        neoStores = createStores();
        // GIVEN a store that has got a node with a dynamic label record
        BatchTransactionApplier applier = buildApplier( LockService.NO_LOCK_SERVICE );
        AtomicLong nodeId = new AtomicLong();
        AtomicLong dynamicLabelRecordId = new AtomicLong();
        apply( applier, transaction( nodeWithDynamicLabelRecord( neoStores, nodeId, dynamicLabelRecordId ) ) );
        assertDynamicLabelRecordInUse( neoStores, dynamicLabelRecordId.get(), true );

        // WHEN applying a transaction where the node is deleted
        apply( applier, transaction( deleteNode( nodeId.get() ) ) );

        // THEN the dynamic label record should also be deleted
        assertDynamicLabelRecordInUse( neoStores, dynamicLabelRecordId.get(), false );
    }

    @Test
    void shouldDeleteDynamicLabelsForDeletedNodeForRecoveredTransaction() throws Throwable
    {
        neoStores = createStores();
        // GIVEN a store that has got a node with a dynamic label record
        BatchTransactionApplier applier = buildApplier( LockService.NO_LOCK_SERVICE );
        AtomicLong nodeId = new AtomicLong();
        AtomicLong dynamicLabelRecordId = new AtomicLong();
        apply( applier, transaction( nodeWithDynamicLabelRecord( neoStores, nodeId, dynamicLabelRecordId ) ) );
        assertDynamicLabelRecordInUse( neoStores, dynamicLabelRecordId.get(), true );

        // WHEN applying a transaction, which has first round-tripped through a log (written then read)
        CommandsToApply transaction = transaction( deleteNode( nodeId.get() ) );
        InMemoryVersionableReadableClosablePositionAwareChannel channel = new InMemoryVersionableReadableClosablePositionAwareChannel();
        writeToChannel( transaction, channel );
        CommandsToApply recoveredTransaction = readFromChannel( channel );
        // and applying that recovered transaction
        apply( applier, recoveredTransaction );

        // THEN should have the dynamic label record should be deleted as well
        assertDynamicLabelRecordInUse( neoStores, dynamicLabelRecordId.get(), false );
    }

    @Test
    void shouldExtractCreatedCommandsInCorrectOrder() throws Throwable
    {
        neoStores = createStores( Config.defaults( dense_node_threshold, 1 ) );
        TransactionRecordState recordState = newTransactionRecordState();
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
    void shouldExtractUpdateCommandsInCorrectOrder() throws Throwable
    {
        neoStores = createStores( Config.defaults( dense_node_threshold, 1 ) );
        TransactionRecordState recordState = newTransactionRecordState();
        long nodeId = 0;
        long relId1 = 1;
        long relId2 = 2;
        long relId3 = 3;
        recordState.nodeCreate( nodeId );
        recordState.relCreate( relId1, 0, nodeId, nodeId );
        recordState.relCreate( relId2, 0, nodeId, nodeId );
        recordState.nodeAddProperty( nodeId, 0, Values.of( 101 ) );
        apply( transaction( recordState ) );

        recordState = newTransactionRecordState();
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
    void shouldIgnoreRelationshipGroupCommandsForGroupThatIsCreatedAndDeletedInThisTx() throws Exception
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
        neoStores = createStores( Config.defaults( dense_node_threshold, 5 ) );
        int relationshipA = 0;
        int relationshipB = 1;
        TransactionRecordState state = newTransactionRecordState();
        state.nodeCreate( 0 );
        state.relCreate( 0, relationshipA, 0, 0 );
        state.relCreate( 1, relationshipA, 0, 0 );
        state.relCreate( 2, relationshipA, 0, 0 );
        state.relCreate( 3, relationshipA, 0, 0 );
        state.relCreate( 4, relationshipB, 0, 0 );
        apply( state );

        // When doing a tx where a relationship of type A for the node is create and rel of type relationshipB is deleted
        state = newTransactionRecordState();
        state.relCreate( 5, relationshipA, 0, 0 ); // here this node should be converted to dense and the groups should be created
        state.relDelete( 4 ); // here the group relationshipB should be delete

        // Then
        Collection<StorageCommand> commands = new ArrayList<>();
        state.extractCommands( commands );
        RelationshipGroupCommand group = singleRelationshipGroupCommand( commands );
        assertEquals( relationshipA, group.getAfter().getType() );
    }

    @Test
    void shouldExtractDeleteCommandsInCorrectOrder() throws Exception
    {
        neoStores = createStores( Config.defaults( dense_node_threshold, 1 ) );
        TransactionRecordState recordState = newTransactionRecordState();
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
        apply( transaction( recordState ) );

        recordState = newTransactionRecordState();
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
        assertCommand( commandIterator.next(), RelationshipCommand.class );
        assertCommand( commandIterator.next(), Command.RelationshipGroupCommand.class );
        assertCommand( commandIterator.next(), NodeCommand.class );
        // property deletes come last.
        assertCommand( commandIterator.next(), PropertyCommand.class );
        assertFalse( commandIterator.hasNext() );
    }

    @Test
    void shouldValidateConstraintIndexAsPartOfExtraction() throws Throwable
    {
        neoStores = createStores();
        // GIVEN
        TransactionRecordState recordState = newTransactionRecordState();

        final long indexId = neoStores.getSchemaStore().nextId();
        final long constraintId = neoStores.getSchemaStore().nextId();

        recordState.schemaRuleCreate( constraintId, true, uniqueForLabel( 1, 1 ).withId( constraintId ).withOwnedIndexId( indexId ) );

        // WHEN
        recordState.extractCommands( new ArrayList<>() );

        // THEN
        verify( integrityValidator ).validateSchemaRule( any() );
    }

    @Test
    void shouldCreateProperBeforeAndAfterPropertyCommandsWhenAddingProperty() throws Exception
    {
        neoStores = createStores();
        // GIVEN
        TransactionRecordState recordState = newTransactionRecordState();

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
    void shouldConvertAddedPropertyToNodePropertyUpdates() throws Exception
    {
        neoStores = createStores();
        // GIVEN
        long nodeId = 0;
        TransactionRecordState recordState = newTransactionRecordState();
        IndexDescriptor rule1 = createIndex( labelIdOne, propertyId1 );
        IndexDescriptor rule2 = createIndex( labelIdOne, propertyId2 );
        IndexDescriptor rule3 = createIndex( labelIdOne, propertyId1, propertyId2 );

        // WHEN
        recordState.nodeCreate( nodeId );
        addLabelsToNode( recordState, nodeId, oneLabelId );
        recordState.nodeAddProperty( nodeId, propertyId1, value1 );
        recordState.nodeAddProperty( nodeId, propertyId2, value2 );
        var updates = indexUpdatesOf( neoStores, recordState );

        // THEN
        assertEquals( asSet(
                add( nodeId, rule1, value1 ),
                add( nodeId, rule2, value2 ),
                add( nodeId, rule3, value1, value2 ) ),
                asSet( single( updates ) ) );
    }

    @Test
    void shouldLockUpdatedNodes() throws Exception
    {
        neoStores = createStores();
        // given
        LockService locks = mock( LockService.class, new Answer<>()
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
        NodeStore nodeStore = neoStores.getNodeStore();
        long[] nodes = { // allocate ids
                nodeStore.nextId(), nodeStore.nextId(), nodeStore.nextId(), nodeStore.nextId(), nodeStore.nextId(), nodeStore.nextId(), nodeStore.nextId(),};

        {
            // create the node records that we will modify in our main tx.
            TransactionRecordState tx = newTransactionRecordState();
            for ( int i = 1; i < nodes.length - 1; i++ )
            {
                tx.nodeCreate( nodes[i] );
            }
            tx.nodeAddProperty( nodes[3], 0, Values.of( "old" ) );
            tx.nodeAddProperty( nodes[4], 0, Values.of( "old" ) );
            BatchTransactionApplier applier = buildApplier( locks );
            apply( applier, transaction( tx ) );
        }
        reset( locks );

        // These are the changes we want to assert locking on
        TransactionRecordState tx = newTransactionRecordState();
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
        BatchTransactionApplier applier = buildApplier( locks );
        apply( applier, transaction( tx ) );

        // then
        // create node, NodeCommand == 1 update
        verify( locks ).acquireNodeLock( nodes[0], LockService.LockType.WRITE_LOCK );
        // add label, NodeCommand == 1 update
        verify( locks ).acquireNodeLock( nodes[1], LockService.LockType.WRITE_LOCK );
        // add property, NodeCommand and PropertyCommand == 2 updates
        verify( locks, times( 2 ) ).acquireNodeLock( nodes[2], LockService.LockType.WRITE_LOCK );
        // update property, in place, PropertyCommand == 1 update
        verify( locks ).acquireNodeLock( nodes[3], LockService.LockType.WRITE_LOCK );
        // remove property, updates the Node and the Property == 2 updates
        verify( locks, times( 2 ) ).acquireNodeLock( nodes[4], LockService.LockType.WRITE_LOCK );
        // delete node, single NodeCommand == 1 update
        verify( locks ).acquireNodeLock( nodes[5], LockService.LockType.WRITE_LOCK );
        // create and add-label goes into the NodeCommand, add property is a PropertyCommand == 2 updates
        verify( locks, times( 2 ) ).acquireNodeLock( nodes[6], LockService.LockType.WRITE_LOCK );
    }

    @Test
    void movingBilaterallyOfTheDenseNodeThresholdIsConsistent() throws Exception
    {
        neoStores = createStores( Config.defaults( dense_node_threshold, 10 ) );
        TransactionRecordState tx = newTransactionRecordState();
        long nodeId = neoStores.getNodeStore().nextId();

        tx.nodeCreate( nodeId );

        int typeA = (int) neoStores.getRelationshipTypeTokenStore().nextId();
        tx.createRelationshipTypeToken( "A", typeA, false );
        createRelationships( neoStores, tx, nodeId, typeA, INCOMING, 20 );

        BatchTransactionApplier applier = buildApplier( LockService.NO_LOCK_SERVICE );
        apply( applier, transaction( tx ) );

        tx = newTransactionRecordState();

        int typeB = 1;
        tx.createRelationshipTypeToken( "B", typeB, false );

        // WHEN
        // i remove enough relationships to become dense and remove enough to become not dense
        long[] relationshipsOfTypeB = createRelationships( neoStores, tx, nodeId, typeB, OUTGOING, 5 );
        for ( long relationshipToDelete : relationshipsOfTypeB )
        {
            tx.relDelete( relationshipToDelete );
        }

        CommandsToApply ptx = transaction( tx );
        apply( applier, ptx );

        // THEN
        // The dynamic label record in before should be the same id as in after, and should be in use
        final AtomicBoolean foundRelationshipGroupInUse = new AtomicBoolean();

        ptx.accept( command -> ((Command) command).handle( new CommandVisitor.Adapter()
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
        assertTrue( foundRelationshipGroupInUse.get(), "Did not create relationship group command" );
    }

    @Test
    void shouldConvertToDenseNodeRepresentationWhenHittingThresholdWithDifferentTypes()
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        neoStores = createStores( Config.defaults( dense_node_threshold, 50 ) );
        TransactionRecordState tx = newTransactionRecordState();
        long nodeId = neoStores.getNodeStore().nextId();
        int typeA = 0;
        int typeB = 1;
        int typeC = 2;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( "A", typeA, false );
        createRelationships( neoStores, tx, nodeId, typeA, OUTGOING, 6 );
        createRelationships( neoStores, tx, nodeId, typeA, INCOMING, 7 );

        tx.createRelationshipTypeToken( "B", typeB, false );
        createRelationships( neoStores, tx, nodeId, typeB, OUTGOING, 8 );
        createRelationships( neoStores, tx, nodeId, typeB, INCOMING, 9 );

        tx.createRelationshipTypeToken( "C", typeC, false );
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
    void shouldConvertToDenseNodeRepresentationWhenHittingThresholdWithTheSameTypeDifferentDirection()
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        neoStores = createStores( Config.defaults( dense_node_threshold, 49 ) );
        TransactionRecordState tx = newTransactionRecordState();
        long nodeId = neoStores.getNodeStore().nextId();
        int typeA = 0;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( "A", typeA, false );
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
    void shouldConvertToDenseNodeRepresentationWhenHittingThresholdWithTheSameTypeSameDirection()
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        neoStores = createStores( Config.defaults( dense_node_threshold, 8 ) );
        TransactionRecordState tx = newTransactionRecordState();
        long nodeId = neoStores.getNodeStore().nextId();
        int typeA = 0;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( "A", typeA, false );
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
    void shouldMaintainCorrectDataWhenDeletingFromDenseNodeWithOneType()
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        neoStores = createStores( Config.defaults( dense_node_threshold, 13 ) );
        TransactionRecordState tx = newTransactionRecordState();
        int nodeId = (int) neoStores.getNodeStore().nextId();
        int typeA = 0;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( "A", typeA, false );
        long[] relationshipsCreated = createRelationships( neoStores, tx, nodeId, typeA, INCOMING, 15 );

        //WHEN
        tx.relDelete( relationshipsCreated[0] );

        // THEN the node should have been converted into a dense node
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeA, 0, 14 );
    }

    @Test
    void shouldMaintainCorrectDataWhenDeletingFromDenseNodeWithManyTypes()
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        neoStores = createStores( Config.defaults( dense_node_threshold, 1 ) );
        TransactionRecordState tx = newTransactionRecordState();
        long nodeId = neoStores.getNodeStore().nextId();
        int typeA = 0;
        int typeB = 12;
        int typeC = 600;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( "A", typeA, false );
        long[] relationshipsCreatedAIncoming = createRelationships( neoStores, tx, nodeId, typeA, INCOMING, 1 );
        long[] relationshipsCreatedAOutgoing = createRelationships( neoStores, tx, nodeId, typeA, OUTGOING, 1 );

        tx.createRelationshipTypeToken( "B", typeB, false );
        long[] relationshipsCreatedBIncoming = createRelationships( neoStores, tx, nodeId, typeB, INCOMING, 1 );
        long[] relationshipsCreatedBOutgoing = createRelationships( neoStores, tx, nodeId, typeB, OUTGOING, 1 );

        tx.createRelationshipTypeToken( "C", typeC, false );
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
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(), typeA );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeB, 1, 1 );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeC, 1, 1 );

        // WHEN
        tx.relDelete( relationshipsCreatedBIncoming[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(), typeA );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeB, 1, 0 );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeC, 1, 1 );

        // WHEN
        tx.relDelete( relationshipsCreatedBOutgoing[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(), typeA );
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(), typeB );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeC, 1, 1 );

        // WHEN
        tx.relDelete( relationshipsCreatedCIncoming[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(), typeA );
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(), typeB );
        assertDenseRelationshipCounts( recordChangeSet, nodeId, typeC, 1, 0 );

        // WHEN
        tx.relDelete( relationshipsCreatedCOutgoing[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(), typeA );
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(), typeB );
        assertRelationshipGroupDoesNotExist( recordChangeSet, recordChangeSet.getNodeRecords().getOrLoad( nodeId, null ).forReadingData(), typeC );
    }

    @Test
    void shouldSortRelationshipGroups() throws Throwable
    {
        int type5 = 5;
        int type10 = 10;
        int type15 = 15;
        neoStores = createStores( Config.defaults( dense_node_threshold, 1 ) );
        {
            TransactionRecordState recordState = newTransactionRecordState();
            neoStores.getRelationshipTypeTokenStore().setHighId( 16 );

            recordState.createRelationshipTypeToken( "5", type5, false );
            recordState.createRelationshipTypeToken( "10", type10, false );
            recordState.createRelationshipTypeToken( "15", type15, false );
            apply( transaction( recordState ) );
        }

        long nodeId = neoStores.getNodeStore().nextId();
        {
            long otherNode1Id = neoStores.getNodeStore().nextId();
            long otherNode2Id = neoStores.getNodeStore().nextId();
            TransactionRecordState recordState = newTransactionRecordState();
            recordState.nodeCreate( nodeId );
            recordState.nodeCreate( otherNode1Id );
            recordState.nodeCreate( otherNode2Id );
            recordState.relCreate( neoStores.getRelationshipStore().nextId(), type10, nodeId, otherNode1Id );
            // This relationship will cause the switch to dense
            recordState.relCreate( neoStores.getRelationshipStore().nextId(), type10, nodeId, otherNode2Id );

            apply( transaction( recordState ) );

            // Just a little validation of assumptions
            assertRelationshipGroupsInOrder( neoStores, nodeId, type10 );
        }

        // WHEN inserting a relationship of type 5
        {
            TransactionRecordState recordState = newTransactionRecordState();
            long otherNodeId = neoStores.getNodeStore().nextId();
            recordState.nodeCreate( otherNodeId );
            recordState.relCreate( neoStores.getRelationshipStore().nextId(), type5, nodeId, otherNodeId );
            apply( transaction( recordState ) );

            // THEN that group should end up first in the chain
            assertRelationshipGroupsInOrder( neoStores, nodeId, type5, type10 );
        }

        // WHEN inserting a relationship of type 15
        {
            TransactionRecordState recordState = newTransactionRecordState();
            long otherNodeId = neoStores.getNodeStore().nextId();
            recordState.nodeCreate( otherNodeId );
            recordState.relCreate( neoStores.getRelationshipStore().nextId(), type15, nodeId, otherNodeId );
            apply( transaction( recordState ) );

            // THEN that group should end up last in the chain
            assertRelationshipGroupsInOrder( neoStores, nodeId, type5, type10, type15 );
        }
    }

    @Test
    void shouldPrepareRelevantRecords() throws Exception
    {
        PrepareTrackingRecordFormats format = new PrepareTrackingRecordFormats( Standard.LATEST_RECORD_FORMATS );
        neoStores = createStores( Config.defaults( dense_node_threshold, 1 ), format );
        // WHEN
        TransactionRecordState state = newTransactionRecordState();
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

    @Test
    void preparingIndexRulesMustMarkSchemaRecordAsChanged() throws Exception
    {
        neoStores = createStores();
        TransactionRecordState state = newTransactionRecordState();
        long ruleId = neoStores.getSchemaStore().nextId();
        IndexDescriptor rule = IndexPrototype.forSchema( forLabel( 0, 1 ) ).withName( "index_" + ruleId ).materialise( ruleId );
        state.schemaRuleCreate( ruleId, false, rule );

        List<StorageCommand> commands = new ArrayList<>();
        state.extractCommands( commands );

        assertThat( commands.size() ).isEqualTo( 1 );
        SchemaRuleCommand command = (SchemaRuleCommand) commands.get( 0 );
        assertThat( command.getMode() ).isEqualTo( Command.Mode.CREATE );
        assertThat( command.getSchemaRule() ).isEqualTo( rule );
        assertThat( command.getKey() ).isEqualTo( ruleId );
        assertThat( command.getBefore().inUse() ).isEqualTo( false );
        assertThat( command.getAfter().inUse() ).isEqualTo( true );
        assertThat( command.getAfter().isConstraint() ).isEqualTo( false );
        assertThat( command.getAfter().isCreated() ).isEqualTo( true );
        assertThat( command.getAfter().getNextProp() ).isEqualTo( Record.NO_NEXT_PROPERTY.longValue() );
    }

    @Test
    void preparingConstraintRulesMustMarkSchemaRecordAsChanged() throws Exception
    {
        neoStores = createStores();
        TransactionRecordState state = newTransactionRecordState();
        long ruleId = neoStores.getSchemaStore().nextId();
        ConstraintDescriptor rule = ConstraintDescriptorFactory.existsForLabel( 0, 1 ).withId( ruleId );
        state.schemaRuleCreate( ruleId, true, rule );

        List<StorageCommand> commands = new ArrayList<>();
        state.extractCommands( commands );

        assertThat( commands.size() ).isEqualTo( 1 );
        SchemaRuleCommand command = (SchemaRuleCommand) commands.get( 0 );
        assertThat( command.getMode() ).isEqualTo( Command.Mode.CREATE );
        assertThat( command.getSchemaRule() ).isEqualTo( rule );
        assertThat( command.getKey() ).isEqualTo( ruleId );
        assertThat( command.getBefore().inUse() ).isEqualTo( false );
        assertThat( command.getAfter().inUse() ).isEqualTo( true );
        assertThat( command.getAfter().isConstraint() ).isEqualTo( true );
        assertThat( command.getAfter().isCreated() ).isEqualTo( true );
        assertThat( command.getAfter().getNextProp() ).isEqualTo( Record.NO_NEXT_PROPERTY.longValue() );
    }

    @Test
    void settingSchemaRulePropertyMustUpdateSchemaRecordIfChainHeadChanges() throws Exception
    {
        neoStores = createStores();
        TransactionRecordState state = newTransactionRecordState();
        long ruleId = neoStores.getSchemaStore().nextId();
        IndexDescriptor rule = IndexPrototype.forSchema( forLabel( 0, 1 ) ).withName( "index_" + ruleId ).materialise( ruleId );
        state.schemaRuleCreate( ruleId, false, rule );

        apply( state );

        state = newTransactionRecordState();
        state.schemaRuleSetProperty( ruleId, 42, Values.booleanValue( true ), rule );
        List<StorageCommand> commands = new ArrayList<>();
        state.extractCommands( commands );

        assertThat( commands.size() ).isEqualTo( 2 );

        PropertyCommand propCmd = (PropertyCommand) commands.get( 0 ); // Order matters. Props added before schema.
        assertThat( propCmd.getSchemaRuleId() ).isEqualTo( ruleId );
        assertThat( propCmd.getBefore().inUse() ).isEqualTo( false );
        assertThat( propCmd.getAfter().inUse() ).isEqualTo( true );
        assertThat( propCmd.getAfter().isCreated() ).isEqualTo( true );
        assertThat( propCmd.getAfter().getSchemaRuleId() ).isEqualTo( ruleId );

        SchemaRuleCommand schemaCmd = (SchemaRuleCommand) commands.get( 1 );
        assertThat( schemaCmd.getSchemaRule() ).isEqualTo( rule );
        assertThat( schemaCmd.getBefore().inUse() ).isEqualTo( true );
        assertThat( schemaCmd.getBefore().getNextProp() ).isEqualTo( Record.NO_NEXT_PROPERTY.longValue() );
        assertThat( schemaCmd.getAfter().inUse() ).isEqualTo( true );
        assertThat( schemaCmd.getAfter().isCreated() ).isEqualTo( false );
        assertThat( schemaCmd.getAfter().getNextProp() ).isEqualTo( propCmd.getKey() );

        apply( transaction( commands ) );

        state = newTransactionRecordState();
        state.schemaRuleSetProperty( ruleId, 42, Values.booleanValue( false ), rule );
        commands.clear();
        state.extractCommands( commands );

        assertThat( commands.size() ).isEqualTo( 1 );

        propCmd = (PropertyCommand) commands.get( 0 );
        assertThat( propCmd.getSchemaRuleId() ).isEqualTo( ruleId );
        assertThat( propCmd.getBefore().inUse() ).isEqualTo( true );
        assertThat( propCmd.getAfter().inUse() ).isEqualTo( true );
        assertThat( propCmd.getAfter().isCreated() ).isEqualTo( false );
    }

    @Test
    void deletingSchemaRuleMustAlsoDeletePropertyChain() throws Exception
    {
        neoStores = createStores();
        TransactionRecordState state = newTransactionRecordState();
        long ruleId = neoStores.getSchemaStore().nextId();
        IndexDescriptor rule = IndexPrototype.forSchema( forLabel( 0, 1 ) ).withName( "index_" + ruleId ).materialise( ruleId );
        state.schemaRuleCreate( ruleId, false, rule );
        state.schemaRuleSetProperty( ruleId, 42, Values.booleanValue( true ), rule );

        apply( state );

        state = newTransactionRecordState();
        state.schemaRuleDelete( ruleId, rule );

        List<StorageCommand> commands = new ArrayList<>();
        state.extractCommands( commands );

        assertThat( commands.size() ).isEqualTo( 2 );
        SchemaRuleCommand schemaCmd = (SchemaRuleCommand) commands.get( 0 ); // Order matters. Rule deletes before property deletes.
        assertThat( schemaCmd.getKey() ).isEqualTo( ruleId );
        assertThat( schemaCmd.getBefore().inUse() ).isEqualTo( true );
        assertThat( schemaCmd.getAfter().inUse() ).isEqualTo( false );

        PropertyCommand propCmd = (PropertyCommand) commands.get( 1 );
        assertThat( propCmd.getKey() ).isEqualTo( schemaCmd.getBefore().getNextProp() );
        assertThat( propCmd.getBefore().inUse() ).isEqualTo( true );
        assertThat( propCmd.getAfter().inUse() ).isEqualTo( false );
    }

    @Test
    void settingIndexOwnerMustAlsoUpdateIndexRule() throws Exception
    {
        neoStores = createStores();
        TransactionRecordState state = newTransactionRecordState();
        long ruleId = neoStores.getSchemaStore().nextId();
        IndexDescriptor rule = IndexPrototype.uniqueForSchema( forLabel( 0, 1 ) ).withName( "index_" + ruleId ).materialise( ruleId );
        state.schemaRuleCreate( ruleId, false, rule );

        apply( state );

        state = newTransactionRecordState();
        state.schemaRuleSetIndexOwner( rule, 13, 42, Values.longValue( 13 ) );
        List<StorageCommand> commands = new ArrayList<>();
        state.extractCommands( commands );

        assertThat( commands.size() ).isEqualTo( 2 );

        PropertyCommand propCmd = (PropertyCommand) commands.get( 0 ); // Order matters. Props added before schema.
        assertThat( propCmd.getSchemaRuleId() ).isEqualTo( ruleId );
        assertThat( propCmd.getBefore().inUse() ).isEqualTo( false );
        assertThat( propCmd.getAfter().inUse() ).isEqualTo( true );
        assertThat( propCmd.getAfter().isCreated() ).isEqualTo( true );
        assertThat( propCmd.getAfter().getSchemaRuleId() ).isEqualTo( ruleId );

        SchemaRuleCommand schemaCmd = (SchemaRuleCommand) commands.get( 1 );
        assertThat( schemaCmd.getSchemaRule() ).isEqualTo( rule );
        assertThat( schemaCmd.getBefore().inUse() ).isEqualTo( true );
        assertThat( schemaCmd.getBefore().getNextProp() ).isEqualTo( Record.NO_NEXT_PROPERTY.longValue() );
        assertThat( schemaCmd.getAfter().inUse() ).isEqualTo( true );
        assertThat( schemaCmd.getAfter().isCreated() ).isEqualTo( false );
        assertThat( schemaCmd.getAfter().getNextProp() ).isEqualTo( propCmd.getKey() );
    }

    /**
     * This is important because we have transaction appliers that look for the schema record changes and inspect the attached schema rule.
     * These appliers will not know what to do with the modified property record. Specifically, the index activator needs to observe the schema record
     * update when an index owner is attached to it.
     */
    @Test
    void settingIndexOwnerMustAlsoUpdateIndexRuleEvenIfIndexOwnerPropertyFitsInExistingPropertyChain() throws Exception
    {
        neoStores = createStores();
        TransactionRecordState state = newTransactionRecordState();
        long ruleId = neoStores.getSchemaStore().nextId();
        IndexDescriptor rule = IndexPrototype.uniqueForSchema( forLabel( 0, 1 ) ).withName( "constraint_" + ruleId ).materialise( ruleId );
        state.schemaRuleCreate( ruleId, false, rule );
        state.schemaRuleSetProperty( ruleId, 42, Values.booleanValue( true ), rule );

        apply( state );

        state = newTransactionRecordState();
        state.schemaRuleSetIndexOwner( rule, 13, 56, Values.longValue( 13 ) );
        List<StorageCommand> commands = new ArrayList<>();
        state.extractCommands( commands );

        assertThat( commands.size() ).isEqualTo( 2 );

        PropertyCommand propCmd = (PropertyCommand) commands.get( 0 ); // Order matters. Props added before schema.
        assertThat( propCmd.getSchemaRuleId() ).isEqualTo( ruleId );
        assertThat( propCmd.getBefore().inUse() ).isEqualTo( true );
        assertThat( propCmd.getAfter().inUse() ).isEqualTo( true );
        assertThat( propCmd.getAfter().isCreated() ).isEqualTo( false );
        assertThat( propCmd.getAfter().getSchemaRuleId() ).isEqualTo( ruleId );

        SchemaRuleCommand schemaCmd = (SchemaRuleCommand) commands.get( 1 );
        assertThat( schemaCmd.getSchemaRule() ).isEqualTo( rule );
        assertThat( schemaCmd.getBefore().inUse() ).isEqualTo( true );
        assertThat( schemaCmd.getBefore().getNextProp() ).isEqualTo( propCmd.getKey() );
        assertThat( schemaCmd.getAfter().inUse() ).isEqualTo( true );
        assertThat( schemaCmd.getAfter().isCreated() ).isEqualTo( false );
        assertThat( schemaCmd.getAfter().getNextProp() ).isEqualTo( propCmd.getKey() );
    }

    private static void addLabelsToNode( TransactionRecordState recordState, long nodeId, long[] labelIds )
    {
        for ( long labelId : labelIds )
        {
            recordState.addLabelToNode( (int) labelId, nodeId );
        }
    }

    private static void removeLabelsFromNode( TransactionRecordState recordState, long nodeId, long[] labelIds )
    {
        for ( long labelId : labelIds )
        {
            recordState.removeLabelFromNode( (int) labelId, nodeId );
        }
    }

    private static long[] createRelationships( NeoStores neoStores, TransactionRecordState tx, long nodeId, int type, Direction direction, int count )
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

    private static void assertRelationshipGroupsInOrder( NeoStores neoStores, long nodeId, int... types )
    {
        NodeStore nodeStore = neoStores.getNodeStore();
        NodeRecord node = nodeStore.getRecord( nodeId, nodeStore.newRecord(), NORMAL );
        assertTrue( node.isDense(), "Node should be dense, is " + node );
        long groupId = node.getNextRel();
        int cursor = 0;
        List<RelationshipGroupRecord> seen = new ArrayList<>();
        while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RecordStore<RelationshipGroupRecord> relationshipGroupStore = neoStores.getRelationshipGroupStore();
            RelationshipGroupRecord group = relationshipGroupStore.getRecord( groupId, relationshipGroupStore.newRecord(), NORMAL );
            seen.add( group );
            assertEquals( types[cursor++], group.getType(), "Invalid type, seen groups so far " + seen );
            groupId = group.getNext();
        }
        assertEquals( types.length, cursor, "Not enough relationship group records found in chain for " + node );
    }

    private Iterable<Iterable<IndexEntryUpdate<IndexDescriptor>>> indexUpdatesOf( NeoStores neoStores, TransactionRecordState state )
            throws IOException, TransactionFailureException
    {
        return indexUpdatesOf( neoStores, transaction( state ) );
    }

    private Iterable<Iterable<IndexEntryUpdate<IndexDescriptor>>> indexUpdatesOf( NeoStores neoStores, CommandsToApply transaction )
            throws IOException
    {
        PropertyCommandsExtractor extractor = new PropertyCommandsExtractor();
        transaction.accept( extractor );

        StorageReader reader = new RecordStorageReader( neoStores );
        List<Iterable<IndexEntryUpdate<IndexDescriptor>>> updates = new ArrayList<>();
        OnlineIndexUpdates onlineIndexUpdates =
                new OnlineIndexUpdates( neoStores.getNodeStore(), schemaCache, new PropertyPhysicalToLogicalConverter( neoStores.getPropertyStore() ), reader );
        onlineIndexUpdates.feed( extractor.getNodeCommands(), extractor.getRelationshipCommands() );
        updates.add( onlineIndexUpdates );
        reader.close();
        return updates;
    }

    private static CommandsToApply transaction( List<StorageCommand> commands )
    {
        return new GroupOfCommands( commands.toArray( new StorageCommand[0] ) );
    }

    private static void assertCommand( StorageCommand next, Class<?> klass )
    {
        assertTrue( klass.isInstance( next ), "Expected " + klass + ". was: " + next );
    }

    @SuppressWarnings( "InfiniteLoopStatement" )
    private static CommandsToApply readFromChannel( ReadableLogChannel channel ) throws IOException
    {
        PhysicalLogCommandReaderV4_0 reader = new PhysicalLogCommandReaderV4_0();
        List<StorageCommand> commands = new ArrayList<>();
        try
        {
            while ( true )
            {
                commands.add( reader.read( channel ) );
            }
        }
        catch ( ReadPastEndException e )
        {
            // reached the end
        }
        return new GroupOfCommands( commands.toArray( new StorageCommand[0] ) );
    }

    private static void writeToChannel( CommandsToApply transaction, FlushableChannel channel ) throws IOException
    {
        transaction.accept( command ->
        {
            command.serialize( channel );
            return false;
        } );
    }

    private TransactionRecordState nodeWithDynamicLabelRecord( NeoStores store, AtomicLong nodeId, AtomicLong dynamicLabelRecordId )
    {
        TransactionRecordState recordState = newTransactionRecordState();

        nodeId.set( store.getNodeStore().nextId() );
        int[] labelIds = new int[20];
        for ( int i = 0; i < labelIds.length; i++ )
        {
            int labelId = (int) store.getLabelTokenStore().nextId();
            recordState.createLabelToken( "Label" + i, labelId, false );
            labelIds[i] = labelId;
        }
        recordState.nodeCreate( nodeId.get() );
        for ( int labelId : labelIds )
        {
            recordState.addLabelToNode( labelId, nodeId.get() );
        }

        // Extract the dynamic label record id (which is also a verification that we allocated one)
        NodeRecord node = single( recordChangeSet.getNodeRecords().changes() ).forReadingData();
        dynamicLabelRecordId.set( single( node.getDynamicLabelRecords() ).getId() );

        return recordState;
    }

    private TransactionRecordState deleteNode( long nodeId )
    {
        TransactionRecordState recordState = newTransactionRecordState();
        recordState.nodeDelete( nodeId );
        return recordState;
    }

    private void apply( BatchTransactionApplier applier, CommandsToApply transaction ) throws Exception
    {
        CommandHandlerContract.apply( applier, transaction );
    }

    private void apply( CommandsToApply transaction ) throws Exception
    {
        BatchTransactionApplier applier = buildApplier( LockService.NO_LOCK_SERVICE );
        apply( applier, transaction );
    }

    private void apply( TransactionRecordState state ) throws Exception
    {
        BatchTransactionApplier applier = buildApplier( LockService.NO_LOCK_SERVICE );
        apply( applier, transaction( state ) );
    }

    private BatchTransactionApplier buildApplier( LockService noLockService )
    {
        Map<IdType,WorkSync<IdGenerator,IdGeneratorUpdateWork>> idGeneratorWorkSyncs = new EnumMap<>( IdType.class );
        for ( IdType idType : IdType.values() )
        {
            idGeneratorWorkSyncs.put( idType, new WorkSync<>( idGeneratorFactory.get( idType ) ) );
        }
        return new NeoStoreBatchTransactionApplier( INTERNAL, neoStores, mock( CacheAccessBackDoor.class ), noLockService, idGeneratorWorkSyncs );
    }

    private TransactionRecordState newTransactionRecordState()
    {
        Loaders loaders = new Loaders( neoStores );
        recordChangeSet = new RecordChangeSet( loaders );
        PropertyTraverser propertyTraverser = new PropertyTraverser();
        RelationshipGroupGetter relationshipGroupGetter = new RelationshipGroupGetter( neoStores.getRelationshipGroupStore() );
        PropertyDeleter propertyDeleter = new PropertyDeleter( propertyTraverser );
        return new TransactionRecordState( neoStores, integrityValidator, recordChangeSet, 0, ResourceLocker.IGNORE,
                new RelationshipCreator( relationshipGroupGetter, neoStores.getRelationshipGroupStore().getStoreHeaderInt() ),
                new RelationshipDeleter( relationshipGroupGetter, propertyDeleter ), new PropertyCreator( neoStores.getPropertyStore(), propertyTraverser ),
                propertyDeleter );
    }

    private static CommandsToApply transaction( TransactionRecordState recordState ) throws TransactionFailureException
    {
        List<StorageCommand> commands = new ArrayList<>();
        recordState.extractCommands( commands );
        return transaction( commands );
    }

    private static void assertDynamicLabelRecordInUse( NeoStores store, long id, boolean inUse )
    {
        DynamicArrayStore dynamicLabelStore = store.getNodeStore().getDynamicLabelStore();
        DynamicRecord record = dynamicLabelStore.getRecord( id, dynamicLabelStore.nextRecord(), FORCE );
        assertEquals( inUse, record.inUse() );
    }

    private static Value string( int length )
    {
        StringBuilder result = new StringBuilder();
        char ch = 'a';
        for ( int i = 0; i < length; i++ )
        {
            result.append( (char) ((ch + (i % 10))) );
        }
        return Values.of( result.toString() );
    }

    private static PropertyCommand singlePropertyCommand( Collection<StorageCommand> commands )
    {
        return (PropertyCommand) single( filter( t -> t instanceof PropertyCommand, commands ) );
    }

    private static RelationshipGroupCommand singleRelationshipGroupCommand( Collection<StorageCommand> commands )
    {
        return (RelationshipGroupCommand) single( filter( t -> t instanceof RelationshipGroupCommand, commands ) );
    }

    private IndexDescriptor createIndex( int labelId, int... propertyKeyIds )
    {
        long id = nextRuleId++;
        IndexDescriptor descriptor = IndexPrototype.forSchema( forLabel( labelId, propertyKeyIds ) ).withName( "index_" + id ).materialise( id );
        schemaCache.addSchemaRule( descriptor );
        return descriptor;
    }

    private LongIterable entityIds( EntityCommandGrouper.Cursor cursor )
    {
        LongArrayList list = new LongArrayList();
        if ( cursor.nextEntity() )
        {
            PropertyCommand propertyCommand;
            do
            {
                // Just get any potential property commands out of the way
                propertyCommand = cursor.nextProperty();
            }
            while ( propertyCommand != null );
            list.add( cursor.currentEntityId() );
        }
        return list;
    }
}
