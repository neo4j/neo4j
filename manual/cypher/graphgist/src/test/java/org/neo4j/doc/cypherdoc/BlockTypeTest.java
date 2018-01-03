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
package org.neo4j.doc.cypherdoc;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.SystemUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult;
import org.neo4j.cypher.javacompat.internal.DocsExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BlockTypeTest
{
    private GraphDatabaseService database;
    private DocsExecutionEngine engine;
    private State state;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final String TEST_BLOCK_START = "[source,querytest]";
    private static final String TEST_BLOCK_MARKER = "----";
    private static final List<String> ADAM_QUERY = Arrays.asList( "[source, cypher]", "----",
            "CREATE (n:Person {name:\"Ad\" + \"am\"})", "RETURN n;", "----" );
    private static final List<String> NOEXEC_QUERY = Arrays.asList( "[source, cypher, noexec=true]", "----",
            "CREATE RETURN n;", "----" );
    private static final List<String> PARAMS = Arrays.asList( "[source, json, role=parameters]", "----",
            "{\"name\": \"Adam\"}", "----" );
    private static final List<String> ADAM_PARAMS_QUERY = Arrays.asList( "[source, cypher]", "----",
            "RETURN {name} = 'Adam';", "----" );
    private static final List<String> RETURN_ONE_QUERY = Arrays.asList( "[source, cypher]", "----",
            "CREATE (n:Person {name:'Alice'}), (m:Person {name:'Bob'}) ", "RETURN m;", "----" );
    private static final List<String> TWO_NODES_ONE_REL = Arrays.asList( "[source, cypher]", "----",
            "CREATE (n:Person {name:'Alice'})-[r:KNOWS {since: 1998}]->(m:Person {name:'Bob'})", "RETURN n,m,r",
            "----" );

    @Before
    public void setup() throws SQLException
    {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        engine = new DocsExecutionEngine( database );
        Connection conn = DriverManager.getConnection( "jdbc:hsqldb:mem:graphgisttests;shutdown=true" );
        conn.setAutoCommit( true );
        state = new State( engine, database, conn, null, "" );
    }

    @After
    public void tearDown()
    {
        database.shutdown();
    }

    @Test
    public void oneLineTitle()
    {
        Block block = Block.getBlock( Arrays.asList( "= Title here =" ) );
        assertThat( block.type, sameInstance( BlockType.TITLE ) );
        String output = block.process( state );
        assertThat( output, containsString( "[[cypherdoc-title-here]]" ) );
        assertThat( output, containsString( "= Title here =" ) );
    }

    @Test
    public void titleWithCharsToIgnore()
    {
        Block block = Block.getBlock( Arrays.asList( "= Title, here?! =" ) );
        assertThat( block.type, sameInstance( BlockType.TITLE ) );
        String output = block.process( state );
        assertThat( output, containsString( "[[cypherdoc-title-here]]" ) );
        assertThat( output, containsString( "= Title, here?! =" ) );
    }

    @Test
    public void ignore_second_level_heading()
    {
        Block block = Block.getBlock( Arrays.asList( "== Title here" ) );
        assertThat( block.type, sameInstance( BlockType.TEXT ) );
        String output = block.process( state );
        assertThat( output, containsString( "== Title here" ) );
    }

    @Test
    public void ignore_second_level_heading_with_id()
    {
        Block block = Block.getBlock( Arrays.asList( "[[my-id]]", "== Title here" ) );
        assertThat( block.type, sameInstance( BlockType.TEXT ) );
        String output = block.process( state );
        assertThat( output, containsString( "[[my-id]]" ) );
        assertThat( output, containsString( "== Title here" ) );
    }

    @Test
    public void queryWithResultAndTest()
    {
        Block block = Block.getBlock( ADAM_QUERY );
        block.process( state );
        assertThat( state.latestResult.text, containsString( "Adam" ) );
        block = Block.getBlock( Arrays.asList( TEST_BLOCK_START, TEST_BLOCK_MARKER, "Adam", TEST_BLOCK_MARKER ) );
        assertThat( block.type, sameInstance( BlockType.QUERYTEST ) );
        block.process( state );
        block = Block.getBlock( Arrays.asList( "// table" ) );
        assertThat( block.type, sameInstance( BlockType.TABLE ) );
        String output = block.process( state );
        assertThat(
                output,
                allOf( containsString( "Adam" ), containsString( "[queryresult]" ), containsString( "Node" ),
                        containsString( "created" ) ) );
    }

    @Test
    public void query_produces_both_dump_and_nodes_and_rels()
    {
        // given
        Block block = Block.getBlock( RETURN_ONE_QUERY );

        // when
        block.process( state );
        Result result = state.latestResult;

        // then
        assertThat( result.text, containsString( "Bob" ) );
        assertThat( result.nodeIds.size(), equalTo( 1 ) );
        assertThat( result.relationshipIds.size(), equalTo( 0 ) );
    }

    @Test
    public void queryWithTestFailure()
    {
        Block block = Block.getBlock( ADAM_QUERY );
        assertThat( block.type, sameInstance( BlockType.CYPHER ) );
        block.process( state );
        block = Block.getBlock( Arrays.asList( TEST_BLOCK_START, TEST_BLOCK_MARKER, "Nobody", TEST_BLOCK_MARKER ) );
        expectedException.expect( TestFailureException.class );
        expectedException.expectMessage( containsString( "Query result doesn't contain the string" ) );
        block.process( state );
    }

    @Test
    public void qurey_with_noexec()
    {
        Block block = Block.getBlock( NOEXEC_QUERY );
        String result = block.process( state );
        assertThat( result, containsString( "CREATE RETURN" ) );
    }

    @Test
    public void query_with_parameters()
    {
        // given
        Block paramsBlock = Block.getBlock( PARAMS );

        // when
        String paramsResult = paramsBlock.process( state );

        // then
        assertThat( paramsResult,
                allOf( containsString( "source,json" ), containsString( "name" ), containsString( "Adam" ) ) );
        assertThat( (String) state.parameters.get( "name" ), equalTo( "Adam" ) );
        assertThat( state.parameters.size(), equalTo( 1 ) );

        // given
        Block queryBlock = Block.getBlock( ADAM_PARAMS_QUERY );

        // when
        queryBlock.process( state );
        String tableResult = Block.getBlock( Arrays.asList( "//table" ) ).process( state );

        // then
        assertThat( tableResult, containsString( "true" ) );
        assertThat( state.parameters.size(), equalTo( 0 ) );
    }

    @Test
    public void graph()
    {
        database.execute( "CREATE (n:Person {name:\"Adam\"});" );
        Block block = Block.getBlock( Arrays.asList( "// graph:xyz" ) );
        assertThat( block.type, sameInstance( BlockType.GRAPH ) );
        String output;
        output = block.process( state );

        assertThat(
                output,
                allOf( startsWith( "[\"dot\"" ), containsString( "Adam" ), containsString( "cypherdoc-xyz" ),
                        containsString( ".svg" ), containsString( "neoviz" ) ) );
    }

    @Test
    public void graph_result_does_not_include_db()
    {
        // given
        Block query = Block.getBlock( RETURN_ONE_QUERY );
        Block graphResult = Block.getBlock( Arrays.asList( "//graph_result" ) );

        // when
        query.process( state );
        String output = graphResult.process( state );

        // then
        assertThat( graphResult.type, sameInstance( BlockType.GRAPH_RESULT ) );
        assertThat( output, containsString( "Bob" ) );
        assertThat( output, not( containsString( "Alice" ) ) );
    }

    @Test
    public void graph_result_includes_relationships()
    {
        // given
        Block query = Block.getBlock( TWO_NODES_ONE_REL );
        Block graphResult = Block.getBlock( Arrays.asList( "//graph_result" ) );

        // when
        query.process( state );
        String output = graphResult.process( state );
        Result result = state.latestResult;

        // then
        assertThat( graphResult.type, sameInstance( BlockType.GRAPH_RESULT ) );
        assertThat(
                output,
                allOf( containsString( "Bob" ), containsString( "Alice" ), containsString( "KNOWS" ),
                        containsString( "1998" ), containsString( "Person" ) ) );
        assertThat( result.nodeIds.size(), equalTo( 2 ) );
        assertThat( result.relationshipIds.size(), equalTo( 1 ) );
    }

    @Test
    public void graphWithoutId()
    {
        database.execute( "CREATE (n:Person {name:\"Adam\"});" );
        Block block = Block.getBlock( Arrays.asList( "//graph" ) );
        assertThat( block.type, sameInstance( BlockType.GRAPH ) );
        String output;
        output = block.process( state );

        assertThat(
            output,
            allOf( startsWith( "[\"dot\"" ), containsString( "Adam" ), containsString( "cypherdoc--" ),
                    containsString( ".svg" ), containsString( "neoviz" ) ) );
    }

    @Test
    public void console()
    {
        Block block = Block.getBlock( Arrays.asList( "// console" ) );
        assertThat( block.type, sameInstance( BlockType.CONSOLE ) );
        String output = block.process( state );
        assertThat(
                output,
                allOf( startsWith( "ifdef::" ), endsWith( "endif::[]" + CypherDoc.EOL ),
                        containsString( "cypherdoc-console" ), containsString( "<p" ), containsString( "<simpara" ),
                        containsString( "html" ) ) );
    }

    @Test
    public void text()
    {
        Block block = Block.getBlock( Arrays.asList( "NOTE: just random asciidoc." ) );
        assertThat( block.type, sameInstance( BlockType.TEXT ) );
        String output = block.process( state );
        assertThat( output, equalTo( "NOTE: just random asciidoc." + CypherDoc.EOL ) );
    }

    @Test
    public void should_match_file_declaration()
    {
        // given
        Block block = Block.getBlock( Arrays.asList( "//file:movies.csv" ) );

        // when
        String output = block.process( state );

        // then
        List<String> operand = new ArrayList<>();
        operand.add( "movies.csv" );

        assertThat( block.type, sameInstance( BlockType.FILE ) );
        assertThat( state.knownFiles, equalTo( operand ) );
        assertThat( output, equalTo( "" ) );
    }

    @Test
    public void should_replace_filenames_in_queries() throws Exception
    {
        assumeFalse( SystemUtils.IS_OS_WINDOWS );
        // given
        List<String> myQuery = Arrays.asList( "[source, cypher]", "----", "LOAD CSV FROM \"my_file.csv\" AS line",
                "RETURN line;", "----" );
        Block block = new Block( myQuery, BlockType.CYPHER );
        DocsExecutionEngine engine = mock( DocsExecutionEngine.class );
        InternalExecutionResult result = mock( InternalExecutionResult.class );
        ResourceIterator iterator = mock( ResourceIterator.class );
        ArgumentCaptor<String> fileQuery = ArgumentCaptor.forClass( String.class );
        ArgumentCaptor<String> httpQuery = ArgumentCaptor.forClass( String.class );

        when( engine.profile( fileQuery.capture(), Matchers.eq( Collections.<String, Object>emptyMap() ) ) ).thenReturn(
                result );

        when( result.javaIterator() ).thenReturn( iterator );

        when( engine.prettify( httpQuery.capture() ) ).thenReturn( "apa" );

        state = new State( engine, database, null, new File( "/dev/null" ), "http://myurl" );
        state.knownFiles.add( "my_file.csv" );


        // when
        block.process( state );

        // then
        assertThat( fileQuery.getValue(), containsString( "file:/dev/null/my_file.csv" ) );
        assertThat( httpQuery.getValue(), containsString( "http://myurl/my_file.csv" ) );
    }
}
