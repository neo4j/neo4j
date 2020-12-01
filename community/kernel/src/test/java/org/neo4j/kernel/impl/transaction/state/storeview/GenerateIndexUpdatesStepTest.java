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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;

import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.SimpleStageControl;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.transaction.state.storeview.GenerateIndexUpdatesStep.GeneratedIndexUpdates;
import org.neo4j.lock.Lock;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StubStorageCursors;
import org.neo4j.values.storable.Value;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.impl.block.factory.primitive.IntPredicates.alwaysTrue;
import static org.neo4j.internal.batchimport.Configuration.DEFAULT;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.EntityTokenUpdate.tokenChanges;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

class GenerateIndexUpdatesStepTest
{
    private static final LongFunction<Lock> NO_LOCKING = id -> null;

    private static final int LABEL = 1;
    private static final int OTHER_LABEL = 2;
    private static final String KEY = "key";
    private static final String OTHER_KEY = "other_key";

    @ValueSource( booleans = {true, false} )
    @ParameterizedTest
    void shouldSendSingleBatchIfBelowMaxSizeThreshold( boolean alsoWrite ) throws Exception
    {
        // given
        StubStorageCursors data = someUniformData( 10 );
        CapturingVisitor<List<EntityUpdates>> propertyUpdates = new CapturingVisitor<>();
        GenerateIndexUpdatesStep<StorageNodeCursor> step =
                new GenerateIndexUpdatesStep<>( new SimpleStageControl(), DEFAULT, data, alwaysTrue(), new NodeCursorBehaviour( data ), new int[]{LABEL},
                        propertyUpdates, null, NO_LOCKING, 1, mebiBytes( 1 ), alsoWrite, PageCacheTracer.NULL, INSTANCE );

        // when
        CapturingBatchSender<GeneratedIndexUpdates> sender = new CapturingBatchSender<>();
        step.process( allNodeIds( data ), sender, NULL );

        // then
        if ( alsoWrite )
        {
            assertThat( sender.batches ).isEmpty();
            assertThat( propertyUpdates.batches.size() ).isEqualTo( 1 );
            assertThat( propertyUpdates.batches.get( 0 ).size() ).isEqualTo( 10 );
        }
        else
        {
            assertThat( sender.batches.size() ).isEqualTo( 1 );
            assertThat( propertyUpdates.batches ).isEmpty();
        }
    }

    @ValueSource( booleans = {true, false} )
    @ParameterizedTest
    void shouldSendBatchesOverMaxByteSizeThreshold( boolean alsoWrite ) throws Exception
    {
        // given
        StubStorageCursors data = someUniformData( 10 );
        CapturingVisitor<List<EntityUpdates>> propertyUpdates = new CapturingVisitor<>();
        GenerateIndexUpdatesStep<StorageNodeCursor> step =
                new GenerateIndexUpdatesStep<>( new SimpleStageControl(), DEFAULT, data, alwaysTrue(), new NodeCursorBehaviour( data ),
                        new int[]{LABEL}, propertyUpdates, null, NO_LOCKING, 1, 100, alsoWrite, PageCacheTracer.NULL, INSTANCE );

        // when
        CapturingBatchSender<GeneratedIndexUpdates> sender = new CapturingBatchSender<>();
        step.process( allNodeIds( data ), sender, NULL );

        // then
        if ( alsoWrite )
        {
            assertThat( propertyUpdates.batches.size() ).isGreaterThan( 1 );
            assertThat( sender.batches ).isEmpty();
        }
        else
        {
            assertThat( propertyUpdates.batches ).isEmpty();
            assertThat( sender.batches.size() ).isGreaterThan( 1 );
        }
    }

    @ValueSource( booleans = {true, false} )
    @ParameterizedTest
    void shouldGenerateEntityPropertyUpdates( boolean alsoWrite ) throws Exception
    {
        // given
        StubStorageCursors data = someUniformData( 10 );
        CapturingVisitor<List<EntityUpdates>> propertyUpdates = new CapturingVisitor<>();
        GenerateIndexUpdatesStep<StorageNodeCursor> step =
                new GenerateIndexUpdatesStep<>( new SimpleStageControl(), DEFAULT, data, alwaysTrue(), new NodeCursorBehaviour( data ), new int[]{LABEL},
                        propertyUpdates, null, NO_LOCKING, 1, mebiBytes( 1 ), alsoWrite, PageCacheTracer.NULL, INSTANCE );
        Set<EntityUpdates> expectedUpdates = new HashSet<>();
        try ( StorageNodeCursor cursor = data.allocateNodeCursor( NULL );
                StoragePropertyCursor propertyCursor = data.allocatePropertyCursor( NULL, INSTANCE ) )
        {
            cursor.scan();
            while ( cursor.next() )
            {
                EntityUpdates.Builder updatesBuilder = EntityUpdates.forEntity( cursor.entityReference(), true ).withTokens( cursor.labels() );
                cursor.properties( propertyCursor );
                while ( propertyCursor.next() )
                {
                    updatesBuilder.added( propertyCursor.propertyKey(), propertyCursor.propertyValue() );
                }
                expectedUpdates.add( updatesBuilder.build() );
            }
        }

        // when
        CapturingBatchSender<GeneratedIndexUpdates> sender = new CapturingBatchSender<>();
        step.process( allNodeIds( data ), sender, NULL );

        // then
        if ( alsoWrite )
        {
            for ( EntityUpdates update : propertyUpdates.batches.get( 0 ) )
            {
                assertThat( expectedUpdates.remove( update ) ).isTrue();
            }
        }
        else
        {
            GeneratedIndexUpdates updates = sender.batches.get( 0 );
            updates.accept( entityPropertyUpdates ->
            {
                for ( EntityUpdates update : entityPropertyUpdates )
                {
                    assertThat( expectedUpdates.remove( update ) ).isTrue();
                }
                return false;
            }, null );
        }
        assertThat( expectedUpdates ).isEmpty();
    }

    @ValueSource( booleans = {true, false} )
    @ParameterizedTest
    void shouldGenerateEntityTokenUpdates( boolean alsoWrite ) throws Exception
    {
        // given
        StubStorageCursors data = someUniformData( 10 );
        CapturingVisitor<List<EntityTokenUpdate>> tokenUpdates = new CapturingVisitor<>();
        GenerateIndexUpdatesStep<StorageNodeCursor> step =
                new GenerateIndexUpdatesStep<>( new SimpleStageControl(), DEFAULT, data, alwaysTrue(), new NodeCursorBehaviour( data ), new int[]{LABEL}, null,
                        tokenUpdates, NO_LOCKING, 1, mebiBytes( 1 ), alsoWrite, PageCacheTracer.NULL, INSTANCE );
        Set<EntityTokenUpdate> expectedUpdates = new HashSet<>();
        try ( StorageNodeCursor cursor = data.allocateNodeCursor( NULL ) )
        {
            cursor.scan();
            while ( cursor.next() )
            {
                expectedUpdates.add( tokenChanges( cursor.entityReference(), EMPTY_LONG_ARRAY, cursor.labels() ) );
            }
        }

        // when
        CapturingBatchSender<GeneratedIndexUpdates> sender = new CapturingBatchSender<>();
        step.process( allNodeIds( data ), sender, NULL );

        // then
        if ( alsoWrite )
        {
            for ( EntityTokenUpdate tokenUpdate : tokenUpdates.batches.get( 0 ) )
            {
                assertThat( expectedUpdates.remove( tokenUpdate ) ).isTrue();
            }
        }
        else
        {
            GeneratedIndexUpdates updates = sender.batches.get( 0 );
            updates.accept( null, entityTokenUpdates ->
            {
                for ( EntityTokenUpdate update : entityTokenUpdates )
                {
                    assertThat( expectedUpdates.remove( update ) ).isTrue();
                }
                return false;
            } );
        }
        assertThat( expectedUpdates ).isEmpty();
    }

    @Test
    void shouldGenerateEntityPropertyUpdatesForRelevantEntityTokens() throws Exception
    {
        // given
        StubStorageCursors data = new StubStorageCursors();
        int numNodes = 10;
        MutableLongSet relevantNodeIds = LongSets.mutable.empty();
        for ( int i = 0; i < numNodes; i++ )
        {
            int labelId = i % 2 == 0 ? LABEL : OTHER_LABEL;
            data.withNode( i ).labels( labelId ).properties( KEY, stringValue( "name_" + i ) );
            if ( labelId == LABEL )
            {
                relevantNodeIds.add( i );
            }
        }
        GenerateIndexUpdatesStep<StorageNodeCursor> step =
                new GenerateIndexUpdatesStep<>( new SimpleStageControl(), DEFAULT, data, alwaysTrue(), new NodeCursorBehaviour( data ), new int[]{LABEL},
                        new CapturingVisitor<>(), null, NO_LOCKING, 1, mebiBytes( 1 ), false, PageCacheTracer.NULL, INSTANCE );

        // when
        CapturingBatchSender<GeneratedIndexUpdates> sender = new CapturingBatchSender<>();
        step.process( allNodeIds( data ), sender, NULL );

        // then
        GeneratedIndexUpdates updates = sender.batches.get( 0 );
        updates.accept( entityPropertyUpdates ->
        {
            for ( EntityUpdates update : entityPropertyUpdates )
            {
                assertThat( relevantNodeIds.remove( update.getEntityId() ) ).isTrue();
            }
            return false;
        }, null );
        assertThat( relevantNodeIds.isEmpty() ).isTrue();
    }

    @ValueSource( booleans = {true, false} )
    @ParameterizedTest
    void shouldGenerateEntityPropertyUpdatesForRelevantPropertyTokens( boolean alsoWrite ) throws Exception
    {
        // given
        StubStorageCursors data = new StubStorageCursors();
        int numNodes = 10;
        MutableLongSet relevantNodeIds = LongSets.mutable.empty();
        for ( int i = 0; i < numNodes; i++ )
        {
            StubStorageCursors.NodeData node = data.withNode( i ).labels( LABEL );
            Map<String,Value> properties = new HashMap<>();
            properties.put( KEY, stringValue( "name_" + i ) );
            if ( i % 2 == 0 )
            {
                properties.put( OTHER_KEY, intValue( i ) );
                relevantNodeIds.add( i );
            }
            node.properties( properties );
        }
        int otherKeyId = data.propertyKeyTokenHolder().getIdByName( OTHER_KEY );
        CapturingVisitor<List<EntityUpdates>> propertyUpdates = new CapturingVisitor<>();
        GenerateIndexUpdatesStep<StorageNodeCursor> step =
                new GenerateIndexUpdatesStep<>( new SimpleStageControl(), DEFAULT, data, pid -> pid == otherKeyId, new NodeCursorBehaviour( data ),
                        new int[]{LABEL}, propertyUpdates, null, NO_LOCKING, 1, mebiBytes( 1 ), alsoWrite, PageCacheTracer.NULL, INSTANCE );

        // when
        CapturingBatchSender<GeneratedIndexUpdates> sender = new CapturingBatchSender<>();
        step.process( allNodeIds( data ), sender, NULL );

        // then
        if ( alsoWrite )
        {
            for ( EntityUpdates update : propertyUpdates.batches.get( 0 ) )
            {
                assertThat( relevantNodeIds.remove( update.getEntityId() ) ).isTrue();
            }
        }
        else
        {
            GeneratedIndexUpdates updates = sender.batches.get( 0 );
            updates.accept( entityPropertyUpdates ->
            {
                for ( EntityUpdates update : entityPropertyUpdates )
                {
                    assertThat( relevantNodeIds.remove( update.getEntityId() ) ).isTrue();
                }
                return false;
            }, null );
        }
        assertThat( relevantNodeIds.isEmpty() ).isTrue();
    }

    @ValueSource( booleans = {true, false} )
    @ParameterizedTest
    void shouldGenerateBothEntityTokenAndPropertyUpdates( boolean alsoWrite ) throws Exception
    {
        // given
        int numNodes = 10;
        StubStorageCursors data = someUniformData( numNodes );
        CapturingVisitor<List<EntityUpdates>> propertyUpdates = new CapturingVisitor<>();
        CapturingVisitor<List<EntityTokenUpdate>> tokenUpdates = new CapturingVisitor<>();
        GenerateIndexUpdatesStep<StorageNodeCursor> step =
                new GenerateIndexUpdatesStep<>( new SimpleStageControl(), DEFAULT, data, alwaysTrue(), new NodeCursorBehaviour( data ), new int[]{LABEL},
                        propertyUpdates, tokenUpdates, NO_LOCKING, 1, mebiBytes( 1 ), alsoWrite, PageCacheTracer.NULL, INSTANCE );

        // when
        CapturingBatchSender<GeneratedIndexUpdates> sender = new CapturingBatchSender<>();
        step.process( allNodeIds( data ), sender, NULL );

        // then
        if ( alsoWrite )
        {
            assertThat( propertyUpdates.batches.size() ).isEqualTo( 1 );
            assertThat( propertyUpdates.batches.get( 0 ).size() ).isEqualTo( numNodes );
            assertThat( tokenUpdates.batches.size() ).isEqualTo( 1 );
            assertThat( tokenUpdates.batches.get( 0 ).size() ).isEqualTo( numNodes );
        }
        else
        {
            GeneratedIndexUpdates updates = sender.batches.get( 0 );
            CapturingVisitor<List<EntityUpdates>> propertyUpdatesCaptor = new CapturingVisitor<>();
            CapturingVisitor<List<EntityTokenUpdate>> tokenUpdatesCaptor = new CapturingVisitor<>();
            updates.accept( propertyUpdatesCaptor, tokenUpdatesCaptor );
            assertThat( propertyUpdatesCaptor.batches.size() ).isEqualTo( 1 );
            assertThat( propertyUpdatesCaptor.batches.get( 0 ).size() ).isEqualTo( numNodes );
            assertThat( tokenUpdatesCaptor.batches.size() ).isEqualTo( 1 );
            assertThat( tokenUpdatesCaptor.batches.get( 0 ).size() ).isEqualTo( numNodes );
        }
    }

    private StubStorageCursors someUniformData( int numNodes )
    {
        StubStorageCursors data = new StubStorageCursors();
        for ( int i = 0; i < numNodes; i++ )
        {
            data.withNode( i ).labels( LABEL ).properties( KEY, stringValue( "name_" + i ) );
        }
        return data;
    }

    private long[] allNodeIds( StubStorageCursors data )
    {
        try ( StorageNodeCursor cursor = data.allocateNodeCursor( NULL ) )
        {
            cursor.scan();
            MutableLongList ids = LongLists.mutable.empty();
            while ( cursor.next() )
            {
                ids.add( cursor.entityReference() );
            }
            return ids.toArray();
        }
    }

    private static class CapturingBatchSender<T> implements BatchSender
    {
        private final List<T> batches = new ArrayList<>();

        @Override
        public void send( Object batch )
        {
            batches.add( (T) batch );
        }
    }

    private static class CapturingVisitor<T> implements Visitor<T,RuntimeException>
    {
        private List<T> batches = new ArrayList<>();

        @Override
        public boolean visit( T element ) throws RuntimeException
        {
            batches.add( element );
            return false;
        }
    }
}
