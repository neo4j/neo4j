/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.internal;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.bolt.v1.runtime.spi.Record;
import org.neo4j.bolt.v1.runtime.spi.RecordStream;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.impl.notification.NotificationCode;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static java.util.Arrays.asList;

import static org.neo4j.graphdb.QueryExecutionType.explained;
import static org.neo4j.graphdb.QueryExecutionType.query;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_ONLY;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_WRITE;
import static org.neo4j.helpers.collection.MapUtil.map;

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

        Result result = mock( Result.class );
        when( result.getQueryExecutionType() ).thenReturn( query( READ_WRITE ) );
        when( result.getQueryStatistics() ).thenReturn( queryStatistics );
        when( result.getNotifications() ).thenReturn( Collections.emptyList() );

        CypherAdapterStream stream = new CypherAdapterStream( result );

        // When
        Map<String,Object> meta = metadataOf( stream );

        // Then
        assertThat( meta.get("type").toString(), equalTo( "rw") );
        assertThat( meta.get("stats"), equalTo( map(
                "nodes-created", 1,
                "nodes-deleted", 2,
                "relationships-created", 3,
                "relationships-deleted", 4,
                "properties-set", 5,
                "indexes-added", 6,
                "indexes-removed", 7,
                "constraints-added", 8,
                "constraints-removed", 9,
                "labels-added", 10,
                "labels-removed", 11
        ) ) );
    }

    @Test
    public void shouldIncludePlanIfPresent() throws Throwable
    {
        // Given
        QueryStatistics queryStatistics = mock( QueryStatistics.class );
        when( queryStatistics.containsUpdates() ).thenReturn( false );
        Result result = mock( Result.class );
        when( result.getQueryExecutionType() ).thenReturn( explained( READ_ONLY ) );
        when( result.getQueryStatistics() ).thenReturn( queryStatistics );
        when( result.getNotifications() ).thenReturn( Collections.emptyList() );
        when( result.getExecutionPlanDescription() ).thenReturn(
                plan("Join", map( "arg1", 1 ), asList( "id1" ),
                plan("Scan", map( "arg2", 1 ), asList("id2")) ) );

        CypherAdapterStream stream = new CypherAdapterStream( result );

        // When
        Map<String,Object> meta = metadataOf( stream );

        // Then
        assertThat( meta.get( "plan" ).toString(), equalTo( "{args={arg1=1}, children=[{args={arg2=1}, children=[], identifiers=[id2], operatorType=Scan}], identifiers=[id1], operatorType=Join}" ) );
    }

    @Test
    public void shouldIncludeProfileIfPresent() throws Throwable
    {
        // Given
        QueryStatistics queryStatistics = mock( QueryStatistics.class );
        when( queryStatistics.containsUpdates() ).thenReturn( false );
        Result result = mock( Result.class );
        when( result.getQueryExecutionType() ).thenReturn( explained( READ_ONLY ) );
        when( result.getQueryStatistics() ).thenReturn( queryStatistics );
        when( result.getNotifications() ).thenReturn( Collections.emptyList() );
        when( result.getExecutionPlanDescription() ).thenReturn(
                plan( "Join", map( "arg1", 1 ), 2, 1, asList( "id1" ),
                        plan( "Scan", map( "arg2", 1 ), 2, 1, asList( "id2" ) ) ) );

        CypherAdapterStream stream = new CypherAdapterStream( result );

        // When
        Map<String,Object> meta = metadataOf( stream );

        // Then
        assertThat( meta.get( "profile" ).toString(), equalTo( "{args={arg1=1}, children=[{args={arg2=1}, children=[], dbHits=2, identifiers=[id2], operatorType=Scan, rows=1}], dbHits=2, identifiers=[id1], operatorType=Join, rows=1}" ));
    }

    @Test
    public void shouldIncludeNotificationsIfPresent() throws Throwable
    {
        // Given
        Result result = mock( Result.class );

        QueryStatistics queryStatistics = mock( QueryStatistics.class );
        when( queryStatistics.containsUpdates() ).thenReturn( false );

        when( result.getQueryStatistics() ).thenReturn( queryStatistics );
        when( result.getQueryExecutionType() ).thenReturn( query( READ_WRITE ) );

        when( result.getNotifications() ).thenReturn( Arrays.<Notification>asList(
                NotificationCode.INDEX_HINT_UNFULFILLABLE.notification( InputPosition.empty ),
                NotificationCode.PLANNER_UNSUPPORTED.notification( new InputPosition( 4, 5, 6 ) )
        ) );
        CypherAdapterStream stream = new CypherAdapterStream( result );

        // When
        Map<String,Object> meta = metadataOf( stream );

        // Then
        assertThat( meta.get( "notifications" ).toString(), equalTo(
         "[{severity=WARNING, description=The hinted index does not exist, please check the schema, code=Neo.ClientError.Schema.IndexNotFound, title=The request (directly or indirectly) referred to an index that does not exist.}, {severity=WARNING, description=Using COST planner is unsupported for this query, please use RULE planner instead, code=Neo.ClientNotification.Statement.PlannerUnsupportedWarning, position={offset=4, column=6, line=5}, title=This query is not supported by the COST planner.}]"
        ) );
    }

    private Map<String,Object> metadataOf( CypherAdapterStream stream ) throws Exception
    {
        final Map<String, Object> meta = new HashMap<>();
        stream.accept( new RecordStream.Visitor()
        {
            @Override
            public void visit( Record record ) throws Exception
            {

            }

            @Override
            public void addMetadata( String key, Object value )
            {
                meta.put( key, value );
            }
        } );
        return meta;
    }

    private static ExecutionPlanDescription plan( final String name, final Map<String, Object> args, final long dbHits, final long rows, final List<String> identifiers, final ExecutionPlanDescription ... children )
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
        }, children );
    }

    private static ExecutionPlanDescription plan( final String name, final Map<String, Object> args, final List<String> identifiers, final ExecutionPlanDescription ... children )
    {
        return plan( name, args, identifiers, null, children );
    }

    private static ExecutionPlanDescription plan( final String name, final Map<String, Object> args, final List<String> identifiers, final
                                                  ExecutionPlanDescription.ProfilerStatistics profile, final ExecutionPlanDescription ... children )
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
                return asList(children);
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
