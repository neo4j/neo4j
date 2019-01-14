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
package org.neo4j.bolt.v1.runtime;

import org.junit.Test;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.impl.notification.NotificationCode;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_ONLY;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_WRITE;
import static org.neo4j.graphdb.QueryExecutionType.explained;
import static org.neo4j.graphdb.QueryExecutionType.query;
import static org.neo4j.helpers.collection.MapUtil.genericMap;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.list;

public class CypherAdapterStreamTest
{
    @Test
    public void shouldIncludeBasicMetadata() throws Throwable
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

        QueryResult result = mock( QueryResult.class );
        when( result.fieldNames() ).thenReturn( new String[0] );
        when( result.executionType() ).thenReturn( query( READ_WRITE ) );
        when( result.queryStatistics() ).thenReturn( queryStatistics );
        when( result.getNotifications() ).thenReturn( Collections.emptyList() );

        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( 0L, 1337L );

        TransactionalContext tc = mock( TransactionalContext.class );
        CypherAdapterStream stream = new CypherAdapterStream( result, clock );

        // When
        MapValue meta = metadataOf( stream );

        // Then
        assertThat( meta.get( "type" ), equalTo( stringValue( "rw" ) ) );
        assertThat( meta.get( "stats" ), equalTo( VirtualValues.map( mapValues(
                "nodes-created", intValue( 1 ),
                "nodes-deleted", intValue( 2 ),
                "relationships-created", intValue( 3 ),
                "relationships-deleted", intValue( 4 ),
                "properties-set", intValue( 5 ),
                "indexes-added", intValue( 6 ),
                "indexes-removed", intValue( 7 ),
                "constraints-added", intValue( 8 ),
                "constraints-removed", intValue( 9 ),
                "labels-added", intValue( 10 ),
                "labels-removed", intValue( 11 ) )
        ) ) );
        assertThat( meta.get( "result_consumed_after" ), equalTo( longValue( 1337L ) ) );
    }

    @Test
    public void shouldIncludePlanIfPresent() throws Throwable
    {
        // Given
        QueryStatistics queryStatistics = mock( QueryStatistics.class );
        when( queryStatistics.containsUpdates() ).thenReturn( false );
        QueryResult result = mock( QueryResult.class );
        when( result.fieldNames() ).thenReturn( new String[0] );
        when( result.executionType() ).thenReturn( explained( READ_ONLY ) );
        when( result.queryStatistics() ).thenReturn( queryStatistics );
        when( result.getNotifications() ).thenReturn( Collections.emptyList() );
        when( result.executionPlanDescription() ).thenReturn(
                plan( "Join", map( "arg1", 1 ), singletonList( "id1" ),
                        plan( "Scan", map( "arg2", 1 ), singletonList( "id2" ) ) ) );

        TransactionalContext tc = mock( TransactionalContext.class );
        CypherAdapterStream stream = new CypherAdapterStream( result, Clock.systemUTC() );

        // When
        MapValue meta = metadataOf( stream );

        // Then
        Map<String,AnyValue> expectedChild = mapValues(
                "args", VirtualValues.map( mapValues( "arg2", intValue( 1 ) ) ),
                "identifiers", list( stringValue( "id2" ) ),
                "operatorType", stringValue( "Scan" ),
                "children", VirtualValues.EMPTY_LIST
        );
        Map<String,AnyValue> expectedPlan = mapValues(
                "args", VirtualValues.map( mapValues( "arg1", intValue( 1 ) ) ),
                "identifiers", list( stringValue( "id1" ) ),
                "operatorType", stringValue( "Join" ),
                "children", list( VirtualValues.map( expectedChild ) )
        );
        assertThat( meta.get( "plan" ), equalTo( VirtualValues.map( expectedPlan ) ) );
    }

    @Test
    public void shouldIncludeProfileIfPresent() throws Throwable
    {
        // Given
        QueryStatistics queryStatistics = mock( QueryStatistics.class );
        when( queryStatistics.containsUpdates() ).thenReturn( false );
        QueryResult result = mock( QueryResult.class );
        when( result.fieldNames() ).thenReturn( new String[0] );
        when( result.executionType() ).thenReturn( explained( READ_ONLY ) );
        when( result.queryStatistics() ).thenReturn( queryStatistics );
        when( result.getNotifications() ).thenReturn( Collections.emptyList() );
        when( result.executionPlanDescription() ).thenReturn(
                plan( "Join", map( "arg1", 1 ), 2, 4, 3, 1, singletonList( "id1" ),
                        plan( "Scan", map( "arg2", 1 ), 2, 4, 7, 1, singletonList( "id2" ) ) ) );

        TransactionalContext tc = mock( TransactionalContext.class );
        CypherAdapterStream stream = new CypherAdapterStream( result, Clock.systemUTC() );

        // When
        MapValue meta = metadataOf( stream );

        // Then
        Map<String,AnyValue> expectedChild = mapValues(
                "args", VirtualValues.map( mapValues( "arg2", intValue( 1 ) ) ),
                "identifiers", list( stringValue( "id2" ) ),
                "operatorType", stringValue( "Scan" ),
                "children", VirtualValues.EMPTY_LIST,
                "rows", longValue( 1L ),
                "dbHits", longValue( 2L ),
                "pageCacheHits", longValue( 4L ),
                "pageCacheMisses", longValue( 7L ),
                "pageCacheHitRatio", doubleValue( 4.0 / 11 )
        );

        Map<String,AnyValue> expectedProfile = mapValues(
                "args", VirtualValues.map( mapValues( "arg1", intValue( 1 ) ) ),
                "identifiers", list( stringValue( "id1" ) ),
                "operatorType", stringValue( "Join" ),
                "children", list( VirtualValues.map( expectedChild ) ),
                "rows", longValue( 1L ),
                "dbHits", longValue( 2L ),
                "pageCacheHits", longValue( 4L ),
                "pageCacheMisses", longValue( 3L ),
                "pageCacheHitRatio", doubleValue( 4.0 / 7 )
        );

        assertMapEqualsWithDelta( (MapValue) meta.get( "profile" ), VirtualValues.map( expectedProfile ), 0.0001 );
    }

    private Map<String,AnyValue> mapValues( Object... values )
    {
        return genericMap( values );
    }

    @Test
    public void shouldIncludeNotificationsIfPresent() throws Throwable
    {
        // Given
        QueryResult result = mock( QueryResult.class );
        when( result.fieldNames() ).thenReturn( new String[0] );

        QueryStatistics queryStatistics = mock( QueryStatistics.class );
        when( queryStatistics.containsUpdates() ).thenReturn( false );

        when( result.queryStatistics() ).thenReturn( queryStatistics );
        when( result.executionType() ).thenReturn( query( READ_WRITE ) );

        when( result.getNotifications() ).thenReturn( Arrays.asList(
                NotificationCode.INDEX_HINT_UNFULFILLABLE.notification( InputPosition.empty ),
                NotificationCode.PLANNER_UNSUPPORTED.notification( new InputPosition( 4, 5, 6 ) )
        ) );
        TransactionalContext tc = mock( TransactionalContext.class );
        CypherAdapterStream stream = new CypherAdapterStream( result, Clock.systemUTC() );

        // When
        MapValue meta = metadataOf( stream );

        // Then
        Map<String,AnyValue> msg1 = mapValues(
                "severity", stringValue( "WARNING" ),
                "code", stringValue( "Neo.ClientError.Schema.IndexNotFound" ),
                "title",
                stringValue( "The request (directly or indirectly) referred to an index that does not exist." ),
                "description", stringValue( "The hinted index does not exist, please check the schema" )
        );
        Map<String,AnyValue> msg2 = mapValues(
                "severity", stringValue( "WARNING" ),
                "code", stringValue( "Neo.ClientNotification.Statement.PlannerUnsupportedWarning" ),
                "title", stringValue( "This query is not supported by the COST planner." ),
                "description",
                stringValue( "Using COST planner is unsupported for this query, please use RULE planner instead" ),
                "position", VirtualValues
                        .map( mapValues( "offset", intValue( 4 ), "column", intValue( 6 ), "line", intValue( 5 ) ) )
        );

        assertThat( meta.get( "notifications" ),
                equalTo( list( VirtualValues.map( msg1 ), VirtualValues.map( msg2 ) ) ) );
    }

    private MapValue metadataOf( CypherAdapterStream stream ) throws Exception
    {
        final Map<String,AnyValue> meta = new HashMap<>();
        stream.accept( new BoltResult.Visitor()
        {
            @Override
            public void visit( QueryResult.Record record )
            {

            }

            @Override
            public void addMetadata( String key, AnyValue value )
            {
                meta.put( key, value );
            }
        } );
        return VirtualValues.map( meta );
    }

    private static void assertMapEqualsWithDelta( MapValue a, MapValue b, double delta )
    {
        assertThat( "Map should have same size", a.size(), equalTo( b.size() ) );
        for ( Map.Entry<String,AnyValue> entry : a.entrySet() )
        {
            String key = entry.getKey();
            //assertThat( "Missing key", b.get( key ) != Values.NO_VALUE );
            AnyValue aValue = entry.getValue();
            AnyValue bValue = b.get( key );
            if ( aValue instanceof MapValue )
            {
                assertThat( "Value mismatch", bValue instanceof MapValue );
                assertMapEqualsWithDelta( (MapValue) aValue, (MapValue) bValue, delta );
            }
            else if ( aValue instanceof DoubleValue )
            {
                assertThat( "Value mismatch", ((DoubleValue) aValue).doubleValue(),
                        closeTo( ((DoubleValue) bValue).doubleValue(), delta ) );
            }
            else
            {
                assertThat( "Value mismatch", aValue, equalTo( bValue ) );
            }
        }
    }

    private static ExecutionPlanDescription plan( final String name, final Map<String,Object> args, final long dbHits,
            final long pageCacheHits, final long pageCacheMisses, final long rows, final List<String> identifiers,
            final ExecutionPlanDescription... children )
    {
        return plan( name, args, identifiers, new ExecutionPlanDescription.ProfilerStatistics()
        {
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
}
