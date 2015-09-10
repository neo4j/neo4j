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
package org.neo4j.ndp.runtime.internal;

import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;
import org.neo4j.ndp.runtime.spi.Record;
import org.neo4j.ndp.runtime.spi.RecordStream;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_ONLY;
import static org.neo4j.graphdb.QueryExecutionType.explained;
import static org.neo4j.helpers.collection.MapUtil.map;

public class CypherAdapterStreamTest
{
    @Test
    public void shouldIncludePlanIfPresent() throws Throwable
    {
        // Given
        Result result = mock( Result.class );
        when( result.getQueryExecutionType() ).thenReturn( explained( READ_ONLY ) );
        when( result.getExecutionPlanDescription() ).thenReturn(
                plan("Join", map( "arg1", 1 ), asList( "id1" ),
                    plan("Scan", map( "arg2", 1 ), asList("id2")) ) );

        CypherAdapterStream stream = new CypherAdapterStream( result );

        // When
        Map<String,Object> meta = metadataOf( stream );

        // Then
        assertThat( meta.get( "plan" ).toString(), equalTo( (Object) map(
            "args", map( "arg1", 1 ),
            "children", asList(
                map(
                    "args", map( "arg2", 1 ),
                    "children", asList(),
                    "identifiers", asList( "id2" ),
                    "operatorType", "Scan"
                )
            ),
            "identifiers", asList( "id1" ),
            "operatorType", "Join"
        ).toString() ));
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