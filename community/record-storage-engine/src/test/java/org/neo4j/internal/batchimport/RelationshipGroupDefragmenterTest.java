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
package org.neo4j.internal.batchimport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.BitSet;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.batchimport.staging.ExecutionMonitors;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.format.ForcedSecondaryUnitRecordFormats;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.of;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.store.format.standard.Standard.LATEST_RECORD_FORMATS;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

@Neo4jLayoutExtension
@ExtendWith( RandomExtension.class )
class RelationshipGroupDefragmenterTest
{
    private static final Configuration CONFIG = Configuration.DEFAULT;

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DatabaseLayout databaseLayout;
    @Inject
    private RandomRule random;

    private static Stream<Arguments> parameters()
    {
        return Stream.of(
            of( LATEST_RECORD_FORMATS, 1 ),
            of( new ForcedSecondaryUnitRecordFormats( LATEST_RECORD_FORMATS ), 2 )
        );
    }

    private BatchingNeoStores stores;
    private JobScheduler jobScheduler;
    private int units;

    private void init( RecordFormats format, int units ) throws IOException
    {
        this.units = units;
        jobScheduler = new ThreadPoolJobScheduler();
        stores = BatchingNeoStores.batchingNeoStores( testDirectory.getFileSystem(), databaseLayout, format, CONFIG, NullLogService.getInstance(),
            AdditionalInitialIds.EMPTY, Config.defaults(), jobScheduler );
        stores.createNew();
    }

    @AfterEach
    void stop() throws Exception
    {
        stores.close();
        jobScheduler.close();
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldDefragmentRelationshipGroupsWhenAllDense( RecordFormats format, int units ) throws IOException
    {
        init( format, units );
        // GIVEN some nodes which has their groups scattered
        int nodeCount = 100;
        int relationshipTypeCount = 50;
        RecordStore<RelationshipGroupRecord> groupStore = stores.getTemporaryRelationshipGroupStore();
        RelationshipGroupRecord groupRecord = groupStore.newRecord();
        RecordStore<NodeRecord> nodeStore = stores.getNodeStore();
        NodeRecord nodeRecord = nodeStore.newRecord();
        long cursor = 0;
        for ( int typeId = relationshipTypeCount - 1; typeId >= 0; typeId-- )
        {
            for ( long nodeId = 0; nodeId < nodeCount; nodeId++, cursor++ )
            {
                // next doesn't matter at all, as we're rewriting it anyway
                // firstOut/In/Loop we could use in verification phase later
                groupRecord.initialize( true, typeId, cursor, cursor + 1, cursor + 2, nodeId, 4 );
                groupRecord.setId( groupStore.nextId() );
                groupStore.updateRecord( groupRecord );

                if ( typeId == 0 )
                {
                    // first round also create the nodes
                    nodeRecord.initialize( true, -1, true, groupRecord.getId(), 0 );
                    nodeRecord.setId( nodeId );
                    nodeStore.updateRecord( nodeRecord );
                    nodeStore.setHighestPossibleIdInUse( nodeId );
                }
            }
        }

        // WHEN
        defrag( nodeCount, groupStore );

        // THEN all groups should sit sequentially in the store
        verifyGroupsAreSequentiallyOrderedByNode();
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldDefragmentRelationshipGroupsWhenSomeDense( RecordFormats format, int units ) throws IOException
    {
        init( format, units );
        // GIVEN some nodes which has their groups scattered
        int nodeCount = 100;
        int relationshipTypeCount = 50;
        RecordStore<RelationshipGroupRecord> groupStore = stores.getTemporaryRelationshipGroupStore();
        RelationshipGroupRecord groupRecord = groupStore.newRecord();
        RecordStore<NodeRecord> nodeStore = stores.getNodeStore();
        NodeRecord nodeRecord = nodeStore.newRecord();
        long cursor = 0;
        BitSet initializedNodes = new BitSet();
        for ( int typeId = relationshipTypeCount - 1; typeId >= 0; typeId-- )
        {
            for ( int nodeId = 0; nodeId < nodeCount; nodeId++, cursor++ )
            {
                // Reasoning behind this thing is that we want to have roughly 10% of the nodes dense
                // right from the beginning and then some stray dense nodes coming into this in the
                // middle of the type range somewhere
                double comparison = typeId == 0 || initializedNodes.get( nodeId ) ? 0.1 : 0.001;

                if ( random.nextDouble() < comparison )
                {
                    // next doesn't matter at all, as we're rewriting it anyway
                    // firstOut/In/Loop we could use in verification phase later
                    groupRecord.initialize( true, typeId, cursor, cursor + 1, cursor + 2, nodeId, 4 );
                    groupRecord.setId( groupStore.nextId() );
                    groupStore.updateRecord( groupRecord );

                    if ( !initializedNodes.get( nodeId ) )
                    {
                        nodeRecord.initialize( true, -1, true, groupRecord.getId(), 0 );
                        nodeRecord.setId( nodeId );
                        nodeStore.updateRecord( nodeRecord );
                        nodeStore.setHighestPossibleIdInUse( nodeId );
                        initializedNodes.set( nodeId );
                    }
                }
            }
        }

        // WHEN
        defrag( nodeCount, groupStore );

        // THEN all groups should sit sequentially in the store
        verifyGroupsAreSequentiallyOrderedByNode();
    }

    private void defrag( int nodeCount, RecordStore<RelationshipGroupRecord> groupStore )
    {
        RelationshipGroupDefragmenter.Monitor monitor = mock( RelationshipGroupDefragmenter.Monitor.class );
        RelationshipGroupDefragmenter defragmenter = new RelationshipGroupDefragmenter( CONFIG,
            ExecutionMonitors.invisible(), monitor, NumberArrayFactory.AUTO_WITHOUT_PAGECACHE );

        // Calculation below correlates somewhat to calculation in RelationshipGroupDefragmenter.
        // Anyway we verify below that we exercise the multi-pass bit, which is what we want
        long memory = groupStore.getHighId() * 15 + 200;
        defragmenter.run( memory, stores, nodeCount );

        // Verify that we exercise the multi-pass functionality
        verify( monitor, atLeast( 2 ) ).defragmentingNodeRange( anyLong(), anyLong() );
        verify( monitor, atMost( 10 ) ).defragmentingNodeRange( anyLong(), anyLong() );
    }

    private void verifyGroupsAreSequentiallyOrderedByNode()
    {
        RelationshipGroupStore store = stores.getRelationshipGroupStore();
        long firstId = store.getNumberOfReservedLowIds();
        long groupCount = store.getHighId() - firstId;
        RelationshipGroupRecord groupRecord = store.newRecord();
        PageCursor groupCursor = store.openPageCursorForReading( firstId );
        long highGroupId = store.getHighId();
        long currentNodeId = -1;
        int currentTypeId = -1;
        int newGroupCount = 0;
        int currentGroupLength = 0;
        for ( long id = firstId; id < highGroupId; id++, newGroupCount++ )
        {
            store.getRecordByCursor( id, groupRecord, CHECK, groupCursor );
            if ( !groupRecord.inUse() )
            {
                // This will be the case if we have double record units, just assert that fact
                assertTrue( units > 1 );
                assertTrue( currentGroupLength > 0 );
                currentGroupLength--;
                continue;
            }

            long nodeId = groupRecord.getOwningNode();
            assertTrue(
                nodeId >= currentNodeId, "Expected a group for node >= " + currentNodeId + ", but was " + nodeId + " in " + groupRecord );
            if ( nodeId != currentNodeId )
            {
                currentNodeId = nodeId;
                currentTypeId = -1;
                if ( units > 1 )
                {
                    assertEquals( 0, currentGroupLength );
                }
                currentGroupLength = 0;
            }
            currentGroupLength++;

            assertTrue(
                groupRecord.getNext() == groupRecord.getId() + 1 ||
                    groupRecord.getNext() == Record.NO_NEXT_RELATIONSHIP.intValue(), "Expected this group to have a next of current + " + units + " OR NULL, " +
                    "but was " + groupRecord.toString() );
            assertTrue(
                groupRecord.getType() > currentTypeId, "Expected " + groupRecord + " to have type > " + currentTypeId );
            currentTypeId = groupRecord.getType();
        }
        assertEquals( groupCount, newGroupCount );
    }
}
