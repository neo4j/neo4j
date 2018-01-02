/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.rest.repr;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;

public class CypherResultRepresentationTest
{
    @Test
    @SuppressWarnings("unchecked")
    public void shouldSerializeProfilingResult() throws Exception
    {
        // Given
        String name = "Kalle";

        ExecutionPlanDescription plan = getMockDescription( name );
        ExecutionPlanDescription childPlan = getMockDescription( "child" );
        when( plan.getChildren() ).thenReturn( asList( childPlan ) );
        when( plan.hasProfilerStatistics() ).thenReturn( true );

        ExecutionPlanDescription.ProfilerStatistics stats = mock( ExecutionPlanDescription.ProfilerStatistics.class );
        when( stats.getDbHits() ).thenReturn( 13l );
        when( stats.getRows() ).thenReturn( 25l );

        when( plan.getProfilerStatistics() ).thenReturn( stats );

        Result result = mock( Result.class );
        when( result.hasNext() ).thenReturn( false );
        when( result.columns() ).thenReturn( new ArrayList<String>() );
        when( result.getExecutionPlanDescription() ).thenReturn( plan );

        // When
        Map<String, Object> serialized = serializeToStringThenParseAsToMap( new CypherResultRepresentation( result,
                /*includeStats=*/false, true ) );

        // Then
        Map<String, Object> serializedPlan = (Map<String, Object>) serialized.get( "plan" );
        assertThat( (String) serializedPlan.get( "name" ), equalTo( name ) );
        assertThat( (Integer) serializedPlan.get( "rows" ), is( 25 ) );
        assertThat( (Integer) serializedPlan.get( "dbHits" ), is( 13 ) );

        List<Map<String, Object>> children = (List<Map<String, Object>>) serializedPlan.get( "children" );
        assertThat( children.size(), is( 1 ) );

        Map<String, Object> args = (Map<String, Object>) serializedPlan.get( "args" );
        assertThat( (String) args.get( "argumentKey" ), is( "argumentValue" ) );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldNotIncludePlanUnlessAskedFor() throws Exception
    {
        // Given
        Result result = mock( Result.class );
        when( result.hasNext() ).thenReturn( false );
        when( result.columns() ).thenReturn( new ArrayList<String>() );

        // When
        Map<String, Object> serialized = serializeToStringThenParseAsToMap( new CypherResultRepresentation( result,
                /*includeStats=*/false, false ) );

        // Then
        assertFalse( "Didn't expect to see a plan here", serialized.containsKey( "plan" ) );
    }

    @Rule
    public DatabaseRule database = new ImpermanentDatabaseRule();

    @Test
    public void shouldFormatMapsProperly() throws Exception
    {
        GraphDatabaseService graphdb = database.getGraphDatabaseService();
        Result result = graphdb.execute( "RETURN {one:{two:['wait for it...', {three: 'GO!'}]}}" );
        CypherResultRepresentation representation = new CypherResultRepresentation( result, false, false );

        // When
        Map<String, Object> serialized = serializeToStringThenParseAsToMap( representation );

        // Then
        Map one = (Map) ((Map) ((List) ((List) serialized.get( "data" )).get( 0 )).get( 0 )).get( "one" );
        List two = (List) one.get( "two" );
        assertThat( (String) two.get( 0 ), is( "wait for it..." ) );
        Map foo = (Map) two.get( 1 );
        assertThat( (String) foo.get( "three" ), is( "GO!" ) );
    }

    @Test
    public void shouldRenderNestedEntities() throws Exception
    {
        try ( Transaction ignored = database.getGraphDatabaseService().beginTx() )
        {
            GraphDatabaseService graphdb = database.getGraphDatabaseService();
            graphdb.execute( "CREATE (n {name: 'Sally'}), (m {age: 42}), n-[r:FOO {drunk: false}]->m" );
            Result result = graphdb.execute( "MATCH p=n-[r]->m RETURN n, r, p, {node: n, edge: r, path: p}" );
            CypherResultRepresentation representation = new CypherResultRepresentation( result, false, false );

            // When
            Map<String, Object> serialized = serializeToStringThenParseAsToMap( representation );

            // Then
            Object firstRow = ((List) serialized.get( "data" )).get( 0 );
            Map nested = (Map) ((List) firstRow).get( 3 );
            assertThat( nested.get( "node" ), is( equalTo( ((List) firstRow).get( 0 ) ) ) );
            assertThat( nested.get( "edge" ), is( equalTo( ((List) firstRow).get( 1 ) ) ) );
            assertThat( nested.get( "path" ), is( equalTo( ((List) firstRow).get( 2 ) ) ) );
        }
    }

    private ExecutionPlanDescription getMockDescription( String name )
    {
        ExecutionPlanDescription plan = mock( ExecutionPlanDescription.class );
        when( plan.getName() ).thenReturn( name );
        when( plan.getArguments() ).thenReturn( MapUtil.map( "argumentKey", "argumentValue" ) );
        return plan;
    }

    private Map<String, Object> serializeToStringThenParseAsToMap( CypherResultRepresentation repr ) throws Exception
    {
        OutputFormat format = new OutputFormat( new JsonFormat(), new URI( "http://localhost/" ), null );
        return jsonToMap( format.assemble( repr ) );
    }
}
