/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.counts;

import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.impl.factory.primitive.LongLongMaps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.internal.recordstorage.FlatRelationshipModifications;
import org.neo4j.internal.recordstorage.FlatRelationshipModifications.RelationshipData;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.storageengine.api.txstate.LongDiffSets;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.internal.recordstorage.Command.GroupDegreeCommand.combinedKeyOnGroupAndDirection;
import static org.neo4j.internal.recordstorage.RecordStorageEngineTestUtils.applyLogicalChanges;
import static org.neo4j.internal.recordstorage.RecordStorageEngineTestUtils.openSimpleStorageEngine;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;

@ExtendWith( RandomExtension.class )
@EphemeralPageCacheExtension
class DegreesRebuildFromStoreTest
{
    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory directory;

    @Inject
    private RandomRule random;

    @Test
    void skipNotUsedRecordsOnDegreeStoreRebuild() throws Exception
    {
        // given a dataset containing mixed sparse and dense nodes with relationships in random directions,
        //       where some chains have been marked as having external degrees
        int denseThreshold = dense_node_threshold.defaultValue();
        DatabaseLayout layout = DatabaseLayout.ofFlat( directory.homePath() );
        int[] relationshipTypes;
        MutableLongLongMap expectedDegrees = LongLongMaps.mutable.empty();
        try ( Lifespan life = new Lifespan() )
        {
            RecordStorageEngine storageEngine = openStorageEngine( layout, denseThreshold );
            relationshipTypes = createRelationshipTypes( storageEngine );
            life.add( storageEngine );
            generateData( storageEngine, denseThreshold, relationshipTypes );
            storageEngine.relationshipGroupDegreesStore().accept(
                    ( groupId, direction, degree ) -> expectedDegrees.put( combinedKeyOnGroupAndDirection( groupId, direction ), degree ), NULL );
            assertThat( expectedDegrees.isEmpty() ).isFalse();

            RelationshipGroupStore groupStore = storageEngine.testAccessNeoStores().getRelationshipGroupStore();
            long highId = groupStore.getHighId();
            assertThat( highId ).isGreaterThan( 1 );
            for ( int i = 10; i < highId; i++ )
            {
                RelationshipGroupRecord record = groupStore.getRecord( i, new RelationshipGroupRecord( i ), RecordLoad.ALWAYS, NULL );
                record.setInUse( false );
                groupStore.updateRecord( record, NULL );
            }
            storageEngine.flushAndForce( NULL );
        }

        // when
        directory.getFileSystem().deleteFile( layout.relationshipGroupDegreesStore() );
        try ( Lifespan life = new Lifespan() )
        {
            RecordStorageEngine storageEngine = assertDoesNotThrow( () -> life.add( openStorageEngine( layout, denseThreshold ) ) );

            // then
            storageEngine.relationshipGroupDegreesStore().accept( ( groupId, direction, degree ) ->
            {
                long key = combinedKeyOnGroupAndDirection( groupId, direction );
                assertThat( expectedDegrees.containsKey( key ) ).isTrue();
                long expectedDegree = expectedDegrees.get( key );
                expectedDegrees.remove( key );
                assertThat( degree ).isEqualTo( expectedDegree );
            }, NULL );
            assertThat( expectedDegrees.size() ).isGreaterThan( 0 );
        }
    }

    @Test
    void shouldRebuildDegreesStore() throws Exception
    {
        // given a dataset containing mixed sparse and dense nodes with relationships in random directions,
        //       where some chains have been marked as having external degrees
        int denseThreshold = dense_node_threshold.defaultValue();
        DatabaseLayout layout = DatabaseLayout.ofFlat( directory.homePath() );
        int[] relationshipTypes;
        MutableLongLongMap expectedDegrees = LongLongMaps.mutable.empty();
        try ( Lifespan life = new Lifespan() )
        {
            RecordStorageEngine storageEngine = openStorageEngine( layout, denseThreshold );
            relationshipTypes = createRelationshipTypes( storageEngine );
            life.add( storageEngine );
            generateData( storageEngine, denseThreshold, relationshipTypes );
            storageEngine.relationshipGroupDegreesStore().accept(
                    ( groupId, direction, degree ) -> expectedDegrees.put( combinedKeyOnGroupAndDirection( groupId, direction ), degree ), NULL );
            assertThat( expectedDegrees.isEmpty() ).isFalse();
            storageEngine.flushAndForce( NULL );
        }

        // when
        directory.getFileSystem().deleteFile( layout.relationshipGroupDegreesStore() );
        try ( Lifespan life = new Lifespan() )
        {
            RecordStorageEngine storageEngine = life.add( openStorageEngine( layout, denseThreshold ) );

            // then
            storageEngine.relationshipGroupDegreesStore().accept( ( groupId, direction, degree ) ->
            {
                long key = combinedKeyOnGroupAndDirection( groupId, direction );
                assertThat( expectedDegrees.containsKey( key ) ).isTrue();
                long expectedDegree = expectedDegrees.get( key );
                expectedDegrees.remove( key );
                assertThat( degree ).isEqualTo( expectedDegree );
            }, NULL );
        }
        assertThat( expectedDegrees.size() ).isEqualTo( 0 );
    }

    private static int[] createRelationshipTypes( RecordStorageEngine storageEngine )
    {
        int[] types = new int[3];
        for ( int i = 0; i < types.length; i++ )
        {
            types[i] = (int) storageEngine.testAccessNeoStores().getRelationshipTypeTokenStore().nextId( NULL );
        }
        return types;
    }

    private void generateData( RecordStorageEngine storageEngine, int denseThreshold, int[] relationshipTypes ) throws Exception
    {
        int numNodes = 100;
        long[] nodes = new long[numNodes];
        applyLogicalChanges( storageEngine, ( state, tx ) ->
        {
            NodeStore nodeStore = storageEngine.testAccessNeoStores().getNodeStore();
            for ( int i = 0; i < numNodes; i++ )
            {
                nodes[i] = nodeStore.nextId( NULL );
                tx.visitCreatedNode( nodes[i] );
            }
        } );

        RelationshipStore relationshipStore = storageEngine.testAccessNeoStores().getRelationshipStore();
        List<RelationshipData> relationships = new ArrayList<>();
        int numRelationships = numNodes * denseThreshold;
        for ( int i = 0; i < numRelationships; i++ )
        {
            relationships.add(
                    new RelationshipData( relationshipStore.nextId( NULL ), random.among( relationshipTypes ), random.among( nodes ), random.among( nodes ) ) );
        }
        applyLogicalChanges( storageEngine,
                ( state, tx ) ->
                {
                    NodeState nodeState = mock( NodeState.class );
                    when( nodeState.labelDiffSets() ).thenReturn( LongDiffSets.EMPTY );
                    when( state.getNodeState( anyLong() ) ).thenReturn( nodeState );
                    tx.visitRelationshipModifications( new FlatRelationshipModifications( relationships.toArray( new RelationshipData[0] ) ) );
                } );
    }

    private RecordStorageEngine openStorageEngine( DatabaseLayout layout, int denseThreshold )
    {
        Config config = Config.defaults( dense_node_threshold, denseThreshold );
        return openSimpleStorageEngine( directory.getFileSystem(), pageCache, layout, config );
    }
}
