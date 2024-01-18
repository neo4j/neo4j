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
package org.neo4j.bolt.runtime;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.bolt.runtime.statemachine.impl.BoltAdapterSubscriber;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.impl.notification.NotificationCode;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.v4.messaging.AbstractStreamingMessage.STREAM_LIMIT_UNLIMITED;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_ONLY;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_WRITE;
import static org.neo4j.graphdb.QueryExecutionType.explained;
import static org.neo4j.graphdb.QueryExecutionType.query;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.list;

public class AbstractCypherAdapterStreamTest
{
    @Test
    void shouldPullAll() throws Throwable
    {
        // Given
        QueryExecution queryExecution = mock( QueryExecution.class );
        when( queryExecution.fieldNames() ).thenReturn( new String[0] );
        when( queryExecution.executionType() ).thenReturn( query( READ_WRITE ) );
        when( queryExecution.getNotifications() ).thenReturn( Collections.emptyList() );
        when( queryExecution.await() ).thenReturn( true ).thenReturn( false );

        BoltAdapterSubscriber subscriber = new BoltAdapterSubscriber();
        QueryStatistics queryStatistics = mock( QueryStatistics.class );
        when( queryStatistics.containsUpdates() ).thenReturn( false );
        when( queryStatistics.getNodesCreated() ).thenReturn( 0 );
        when( queryStatistics.getNodesDeleted() ).thenReturn( 0 );
        when( queryStatistics.getRelationshipsCreated() ).thenReturn( 0 );
        when( queryStatistics.getRelationshipsDeleted() ).thenReturn( 0 );
        when( queryStatistics.getPropertiesSet() ).thenReturn( 0 );
        when( queryStatistics.getIndexesAdded() ).thenReturn( 0 );
        when( queryStatistics.getIndexesRemoved() ).thenReturn( 0 );
        when( queryStatistics.getConstraintsAdded() ).thenReturn( 0 );
        when( queryStatistics.getConstraintsRemoved() ).thenReturn( 0 );
        when( queryStatistics.getLabelsAdded() ).thenReturn( 0 );
        when( queryStatistics.getLabelsRemoved() ).thenReturn( 0 );
        subscriber.onResultCompleted( queryStatistics );

        Clock clock = mock( Clock.class );
        var stream = new TestAbstractCypherAdapterStream( queryExecution, subscriber, clock );
        // When
        stream.handleRecords( mock( BoltResult.RecordConsumer.class ), STREAM_LIMIT_UNLIMITED );

        // Then
        verify( queryExecution, times( 2 ) ).request( Long.MAX_VALUE );
        verify( queryExecution, times( 2 ) ).await();
    }

    @Test
    void shouldComputeTimeSpentStreaming() throws Throwable
    {
        // Given
        QueryExecution queryExecution = mock( QueryExecution.class );
        when( queryExecution.fieldNames() ).thenReturn( new String[0] );
        when( queryExecution.executionType() ).thenReturn( query( READ_WRITE ) );
        when( queryExecution.getNotifications() ).thenReturn( Collections.emptyList() );
        when( queryExecution.await() ).thenReturn( true ).thenReturn( false );

        BoltAdapterSubscriber subscriber = new BoltAdapterSubscriber();
        QueryStatistics queryStatistics = mock( QueryStatistics.class );
        when( queryStatistics.containsUpdates() ).thenReturn( false );
        when( queryStatistics.getNodesCreated() ).thenReturn( 0 );
        when( queryStatistics.getNodesDeleted() ).thenReturn( 0 );
        when( queryStatistics.getRelationshipsCreated() ).thenReturn( 0 );
        when( queryStatistics.getRelationshipsDeleted() ).thenReturn( 0 );
        when( queryStatistics.getPropertiesSet() ).thenReturn( 0 );
        when( queryStatistics.getIndexesAdded() ).thenReturn( 0 );
        when( queryStatistics.getIndexesRemoved() ).thenReturn( 0 );
        when( queryStatistics.getConstraintsAdded() ).thenReturn( 0 );
        when( queryStatistics.getConstraintsRemoved() ).thenReturn( 0 );
        when( queryStatistics.getLabelsAdded() ).thenReturn( 0 );
        when( queryStatistics.getLabelsRemoved() ).thenReturn( 0 );
        subscriber.onResultCompleted( queryStatistics );

        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( 0L, 1000L, 1001L, 1002L );
        var stream = new TestAbstractCypherAdapterStream( queryExecution, subscriber, clock );
        // When
        assertThat(stream.handleRecords( mock( BoltResult.RecordConsumer.class ), 1 )).isTrue();
        assertThat(stream.handleRecords( mock( BoltResult.RecordConsumer.class ), 1 )).isFalse();

        // Then
        assertThat( stream.timeSpentStreaming ).isEqualTo( 1001L );
    }

    @Test
    void shouldDiscardAllReadQuery() throws Throwable
    {
        // Given
        QueryExecution queryExecution = mock( QueryExecution.class );
        when( queryExecution.fieldNames() ).thenReturn( new String[]{ "foo" } );
        when( queryExecution.executionType() ).thenReturn( query( READ_ONLY ) );
        when( queryExecution.getNotifications() ).thenReturn( Collections.emptyList() );
        when( queryExecution.await() ).thenReturn( false );

        BoltAdapterSubscriber subscriber = new BoltAdapterSubscriber();
        QueryStatistics queryStatistics = mock( QueryStatistics.class );
        when( queryStatistics.containsUpdates() ).thenReturn( false );
        when( queryStatistics.getNodesCreated() ).thenReturn( 0 );
        when( queryStatistics.getNodesDeleted() ).thenReturn( 0 );
        when( queryStatistics.getRelationshipsCreated() ).thenReturn( 0 );
        when( queryStatistics.getRelationshipsDeleted() ).thenReturn( 0 );
        when( queryStatistics.getPropertiesSet() ).thenReturn( 0 );
        when( queryStatistics.getIndexesAdded() ).thenReturn( 0 );
        when( queryStatistics.getIndexesRemoved() ).thenReturn( 0 );
        when( queryStatistics.getConstraintsAdded() ).thenReturn( 0 );
        when( queryStatistics.getConstraintsRemoved() ).thenReturn( 0 );
        when( queryStatistics.getLabelsAdded() ).thenReturn( 0 );
        when( queryStatistics.getLabelsRemoved() ).thenReturn( 0 );
        subscriber.onResultCompleted( queryStatistics );

        Clock clock = mock( Clock.class );
        var stream = new TestAbstractCypherAdapterStream( queryExecution, subscriber, clock );
        // When
        stream.discardRecords( mock( BoltResult.DiscardingRecordConsumer.class ), STREAM_LIMIT_UNLIMITED );

        // Then
        verify( queryExecution, times( 1 ) ).cancel();
        verify( queryExecution, times( 1 ) ).await();
    }

    @Test
    void shouldDiscardAllReadWriteQuery() throws Throwable
    {
        // Given
        QueryExecution queryExecution = mock( QueryExecution.class );
        when( queryExecution.fieldNames() ).thenReturn( new String[]{ "foo" } );
        when( queryExecution.executionType() ).thenReturn( query( READ_WRITE ) );
        when( queryExecution.getNotifications() ).thenReturn( Collections.emptyList() );
        when( queryExecution.await() ).thenReturn( false );

        BoltAdapterSubscriber subscriber = new BoltAdapterSubscriber();
        QueryStatistics queryStatistics = mock( QueryStatistics.class );
        when( queryStatistics.containsUpdates() ).thenReturn( false );
        when( queryStatistics.getNodesCreated() ).thenReturn( 0 );
        when( queryStatistics.getNodesDeleted() ).thenReturn( 0 );
        when( queryStatistics.getRelationshipsCreated() ).thenReturn( 0 );
        when( queryStatistics.getRelationshipsDeleted() ).thenReturn( 0 );
        when( queryStatistics.getPropertiesSet() ).thenReturn( 0 );
        when( queryStatistics.getIndexesAdded() ).thenReturn( 0 );
        when( queryStatistics.getIndexesRemoved() ).thenReturn( 0 );
        when( queryStatistics.getConstraintsAdded() ).thenReturn( 0 );
        when( queryStatistics.getConstraintsRemoved() ).thenReturn( 0 );
        when( queryStatistics.getLabelsAdded() ).thenReturn( 0 );
        when( queryStatistics.getLabelsRemoved() ).thenReturn( 0 );
        subscriber.onResultCompleted( queryStatistics );

        Clock clock = mock( Clock.class );
        var stream = new TestAbstractCypherAdapterStream( queryExecution, subscriber, clock );
        // When
        stream.discardRecords( mock( BoltResult.DiscardingRecordConsumer.class ), STREAM_LIMIT_UNLIMITED );

        // Then
        verify( queryExecution, times( 1 ) ).request( Long.MAX_VALUE );
        verify( queryExecution, times( 1 ) ).await();
    }

    @Test
    void shouldIncludeBasicMetadata() throws Throwable
    {
        // Given
        QueryStatistics queryStatistics = mock( QueryStatistics.class );
        when( queryStatistics.containsUpdates() ).thenReturn( true );
        when( queryStatistics.getNodesCreated() ).thenReturn( 1 );
        when( queryStatistics.getNodesDeleted() ).thenReturn( 2 );
        when( queryStatistics.getRelationshipsCreated() ).thenReturn( 3 );
        when( queryStatistics.getRelationshipsDeleted() ).thenReturn( 4 );
        when( queryStatistics.getPropertiesSet() ).thenReturn( 5 );
        when( queryStatistics.getIndexesAdded() ).thenReturn( 6 );
        when( queryStatistics.getIndexesRemoved() ).thenReturn( 7 );
        when( queryStatistics.getConstraintsAdded() ).thenReturn( 8 );
        when( queryStatistics.getConstraintsRemoved() ).thenReturn( 9 );
        when( queryStatistics.getLabelsAdded() ).thenReturn( 10 );
        when( queryStatistics.getLabelsRemoved() ).thenReturn( 11 );

        QueryExecution result = mock( QueryExecution.class );
        BoltAdapterSubscriber subscriber = new BoltAdapterSubscriber();
        when( result.fieldNames() ).thenReturn( new String[0] );
        when( result.executionType() ).thenReturn( query( READ_WRITE ) );
        subscriber.onResultCompleted( queryStatistics );
        when( result.getNotifications() ).thenReturn( Collections.emptyList() );

        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( 0L, 1337L );

        var stream = new TestAbstractCypherAdapterStream( result, subscriber, clock );

        // When
        MapValue meta = metadataOf( stream );

        // Then
        assertThat( meta.get( "type" ) ).isEqualTo( stringValue( "rw" ) );
        assertThat( meta.get( "stats" ) ).isEqualTo(
                mapValues( "nodes-created", intValue( 1 ), "nodes-deleted", intValue( 2 ), "relationships-created", intValue( 3 ), "relationships-deleted",
                        intValue( 4 ), "properties-set", intValue( 5 ), "indexes-added", intValue( 6 ), "indexes-removed", intValue( 7 ), "constraints-added",
                        intValue( 8 ), "constraints-removed", intValue( 9 ), "labels-added", intValue( 10 ), "labels-removed", intValue( 11 ) ) );
    }

    @Test
    void shouldIncludeSystemUpdates() throws Throwable
    {
        // Given
        QueryStatistics queryStatistics = mock( QueryStatistics.class );
        when( queryStatistics.containsSystemUpdates() ).thenReturn( true );
        when( queryStatistics.getSystemUpdates() ).thenReturn( 11 );

        QueryExecution result = mock( QueryExecution.class );
        BoltAdapterSubscriber subscriber = new BoltAdapterSubscriber();
        when( result.fieldNames() ).thenReturn( new String[0] );
        when( result.executionType() ).thenReturn( query( READ_WRITE ) );
        subscriber.onResultCompleted( queryStatistics );
        when( result.getNotifications() ).thenReturn( Collections.emptyList() );

        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( 0L, 1337L );

        var stream = new TestAbstractCypherAdapterStream( result, subscriber, clock );

        // When
        MapValue meta = metadataOf( stream );

        // Then
        assertThat( meta.get( "type" ) ).isEqualTo( stringValue( "rw" ) );
        assertThat( meta.get( "stats" ) ).isEqualTo( mapValues( "system-updates", intValue( 11 ) ) );
    }

    @Test
    void shouldIncludePlanIfPresent() throws Throwable
    {
        // Given
        QueryStatistics queryStatistics = mock( QueryStatistics.class );
        when( queryStatistics.containsUpdates() ).thenReturn( false );
        QueryExecution result = mock( QueryExecution.class );
        BoltAdapterSubscriber subscriber = new BoltAdapterSubscriber();
        when( result.fieldNames() ).thenReturn( new String[0] );
        when( result.executionType() ).thenReturn( explained( READ_ONLY ) );
        subscriber.onResultCompleted( queryStatistics );

        when( result.getNotifications() ).thenReturn( Collections.emptyList() );
        when( result.executionPlanDescription() ).thenReturn(
                plan( "Join", map( "arg1", 1 ), singletonList( "id1" ),
                        plan( "Scan", map( "arg2", 1 ), singletonList( "id2" ) ) ) );

        var stream = new TestAbstractCypherAdapterStream( result, subscriber, Clock.systemUTC() );

        // When
        MapValue meta = metadataOf( stream );

        // Then
        MapValue expectedChild = mapValues(
                "args", mapValues( "arg2", intValue( 1 ) ),
                "identifiers", list( stringValue( "id2" ) ),
                "operatorType", stringValue( "Scan" ),
                "children", VirtualValues.EMPTY_LIST
        );
        MapValue expectedPlan = mapValues(
                "args", mapValues( "arg1", intValue( 1 ) ),
                "identifiers", list( stringValue( "id1" ) ),
                "operatorType", stringValue( "Join" ),
                "children", list( expectedChild ) );
        assertThat( meta.get( "plan" ) ).isEqualTo( expectedPlan );
    }

    @Test
    void shouldIncludeProfileIfPresent() throws Throwable
    {
        // Given
        QueryStatistics queryStatistics = mock( QueryStatistics.class );
        when( queryStatistics.containsUpdates() ).thenReturn( false );
        QueryExecution result = mock( QueryExecution.class );
        BoltAdapterSubscriber subscriber = new BoltAdapterSubscriber();
        when( result.fieldNames() ).thenReturn( new String[0] );
        when( result.executionType() ).thenReturn( explained( READ_ONLY ) );
        subscriber.onResultCompleted( queryStatistics );

        when( result.getNotifications() ).thenReturn( Collections.emptyList() );
        when( result.executionPlanDescription() ).thenReturn(
                plan( "Join", map( "arg1", 1 ), 2, 4, 3, 1, 2, singletonList( "id1" ),
                        plan( "Scan", map( "arg2", 1 ), 2, 4, 7, 1, 1, singletonList( "id2" ) ) ) );

        var stream = new TestAbstractCypherAdapterStream( result, subscriber, Clock.systemUTC() );

        // When
        MapValue meta = metadataOf( stream );

        // Then
        MapValue expectedChild = mapValues(
                "args", mapValues( "arg2", intValue( 1 ) ) ,
                "identifiers", list( stringValue( "id2" ) ),
                "operatorType", stringValue( "Scan" ),
                "children", VirtualValues.EMPTY_LIST,
                "rows", longValue( 1L ),
                "dbHits", longValue( 2L ),
                "pageCacheHits", longValue( 4L ),
                "pageCacheMisses", longValue( 7L ),
                "pageCacheHitRatio", doubleValue( 4.0 / 11 ),
                "time", longValue( 1 )
        );

        MapValue expectedProfile = mapValues(
                "args",  mapValues( "arg1", intValue( 1 ) ),
                "identifiers", list( stringValue( "id1" ) ),
                "operatorType", stringValue( "Join" ),
                "children", list( expectedChild ),
                "rows", longValue( 1L ),
                "dbHits", longValue( 2L ),
                "pageCacheHits", longValue( 4L ),
                "pageCacheMisses", longValue( 3L ),
                "pageCacheHitRatio", doubleValue( 4.0 / 7 ),
                "time", longValue( 2 )
        );

        assertMapEqualsWithDelta( (MapValue) meta.get( "profile" ),  expectedProfile, 0.0001 );
    }

    protected static MapValue mapValues( Object... values )
    {
        int i = 0;
        MapValueBuilder builder = new MapValueBuilder();
        while ( i < values.length )
        {
            builder.add( (String) values[i++], (AnyValue) values[i++] );
        }
        return builder.build();
    }

    @Test
    void shouldIncludeNotificationsIfPresent() throws Throwable
    {
        // Given
        QueryExecution result = mock( QueryExecution.class );
        BoltAdapterSubscriber subscriber = new BoltAdapterSubscriber();
        when( result.fieldNames() ).thenReturn( new String[0] );

        QueryStatistics queryStatistics = mock( QueryStatistics.class );
        when( queryStatistics.containsUpdates() ).thenReturn( false );

        subscriber.onResultCompleted( queryStatistics );

        when( result.executionType() ).thenReturn( query( READ_WRITE ) );

        when( result.getNotifications() ).thenReturn( Arrays.asList(
                NotificationCode.INDEX_HINT_UNFULFILLABLE.notification( InputPosition.empty ),
                NotificationCode.RUNTIME_UNSUPPORTED.notification( new InputPosition( 4, 5, 6 ) )
        ) );
        var stream = new TestAbstractCypherAdapterStream( result, subscriber, Clock.systemUTC() );

        // When
        MapValue meta = metadataOf( stream );

        // Then
        MapValue msg1 = mapValues(
                "severity", stringValue( "WARNING" ),
                "code", stringValue( "Neo.ClientError.Schema.IndexNotFound" ),
                "title",
                stringValue( "The request (directly or indirectly) referred to an index that does not exist." ),
                "description", stringValue( "The hinted index does not exist, please check the schema" )
        );
        MapValue msg2 = mapValues(
                "severity", stringValue( "WARNING" ),
                "code", stringValue( "Neo.ClientNotification.Statement.RuntimeUnsupportedWarning" ),
                "title", stringValue( "This query is not supported by the chosen runtime." ),
                "description",
                stringValue( "Selected runtime is unsupported for this query, please use a different runtime instead or fallback to default." ),
                "position", mapValues( "offset", intValue( 4 ), "column", intValue( 6 ), "line", intValue( 5 ) )
        );

        assertThat( meta.get( "notifications" ) ).isEqualTo( list( msg1, msg2 ) );
    }

    protected static MapValue metadataOf( AbstractCypherAdapterStream stream ) throws Throwable
    {
        final MapValueBuilder meta = new MapValueBuilder();
        stream.handleRecords( new BoltResult.DiscardingRecordConsumer()
        {
            @Override
            public void addMetadata( String key, AnyValue value )
            {
                meta.add( key, value );
            }
        }, STREAM_LIMIT_UNLIMITED );
        return meta.build();
    }

    private static void assertMapEqualsWithDelta( MapValue a, MapValue b, double delta )
    {
        assertThat( a.size() ).as( "Map should have same size" ).isEqualTo( b.size() );
        a.foreach( ( key, value ) -> {
            //assertThat( "Missing key", b.get( key ) != Values.NO_VALUE );
            AnyValue aValue = value;
            AnyValue bValue = b.get( key );
            if ( aValue instanceof MapValue )
            {
                assertThat( bValue instanceof MapValue ).as( "Value mismatch" ).isTrue();
                assertMapEqualsWithDelta( (MapValue) aValue, (MapValue) bValue, delta );
            }
            else if ( aValue instanceof DoubleValue )
            {
                assertThat( ((DoubleValue) aValue).doubleValue() ).as( "Value mismatch" ).isCloseTo( ((DoubleValue) bValue).doubleValue(), offset( delta ) );
            }
            else
            {
                assertThat( aValue ).as( "Value mismatch" ).isEqualTo( bValue );
            }
        } );
    }

    private static ExecutionPlanDescription plan(
            final String name,
            final Map<String,Object> args,
            final long dbHits,
            final long pageCacheHits,
            final long pageCacheMisses,
            final long rows,
            final long time,
            final List<String> identifiers,
            final ExecutionPlanDescription... children )
    {
        return plan( name, args, identifiers, new ExecutionPlanDescription.ProfilerStatistics()
        {
            @Override
            public boolean hasRows()
            {
                return true;
            }

            @Override
            public boolean hasDbHits()
            {
                return true;
            }

            @Override
            public boolean hasPageCacheStats()
            {
                return true;
            }

            @Override
            public boolean hasTime()
            {
                return true;
            }

            @Override
            public long getRows()
            {
                return rows;
            }

            @Override
            public long getDbHits()
            {
                return dbHits;
            }

            @Override
            public long getPageCacheHits()
            {
                return pageCacheHits;
            }

            @Override
            public long getPageCacheMisses()
            {
                return pageCacheMisses;
            }

            @Override
            public long getTime()
            {
                return time;
            }
        }, children );
    }

    private static ExecutionPlanDescription plan( final String name, final Map<String,Object> args,
            final List<String> identifiers, final ExecutionPlanDescription... children )
    {
        return plan( name, args, identifiers, null, children );
    }

    private static ExecutionPlanDescription plan( final String name, final Map<String,Object> args,
            final List<String> identifiers, final ExecutionPlanDescription.ProfilerStatistics profile,
            final ExecutionPlanDescription... children )
    {
        return new ExecutionPlanDescription()
        {
            @Override
            public String getName()
            {
                return name;
            }

            @Override
            public List<ExecutionPlanDescription> getChildren()
            {
                return asList( children );
            }

            @Override
            public Map<String,Object> getArguments()
            {
                return args;
            }

            @Override
            public Set<String> getIdentifiers()
            {
                return new HashSet<>( identifiers );
            }

            @Override
            public boolean hasProfilerStatistics()
            {
                return profile != null;
            }

            @Override
            public ProfilerStatistics getProfilerStatistics()
            {
                return profile;
            }
        };
    }

    private static class TestAbstractCypherAdapterStream extends AbstractCypherAdapterStream
    {
        private long timeSpentStreaming;
        TestAbstractCypherAdapterStream( QueryExecution queryExecution, BoltAdapterSubscriber querySubscriber, Clock clock )
        {
            super( queryExecution, querySubscriber, clock );
        }

        @Override
        protected void addDatabaseName( RecordConsumer recordConsumer )
        {
        }

        @Override
        protected void addRecordStreamingTime( long time, RecordConsumer recordConsumer )
        {
            this.timeSpentStreaming = time;
        }
    }
}
