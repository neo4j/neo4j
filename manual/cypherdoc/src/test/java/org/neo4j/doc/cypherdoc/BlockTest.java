/**
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
package org.neo4j.doc.cypherdoc;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

public class BlockTest
{
    private GraphDatabaseService database;
    private ExecutionEngine engine;
    private State state;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final String COMMENT_BLOCK = "////";
    private static final List<String> ADAM_QUERY = Arrays.asList( "[source, cypher]", "----",
            "CREATE (n:Person {name:\"Ad\" + \"am\"})", "RETURN n;", "----" );

    @Before
    public void setup()
    {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        engine = new ExecutionEngine( database );
        state = new State( engine, database );
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
    public void twoLineTitle()
    {
        Block block = Block.getBlock( Arrays.asList( "Title here", "==========" ) );
        assertThat( block.type, sameInstance( BlockType.TITLE ) );
        String output = block.process( state );
        assertThat( output, containsString( "[[cypherdoc-title-here]]" ) );
        assertThat( output, containsString( "= Title here =" ) );
    }

    @Test
    public void queryWithResultAndTest()
    {
        Block block = Block.getBlock( ADAM_QUERY );
        block.process( state );
        assertThat( state.latestResult.text, containsString( "Adam" ) );
        block = Block.getBlock( Arrays.asList( COMMENT_BLOCK, "Adam", COMMENT_BLOCK ) );
        assertThat( block.type, sameInstance( BlockType.TEST ) );
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
    public void queryWithTestFailure()
    {
        Block block = Block.getBlock( ADAM_QUERY );
        assertThat( block.type, sameInstance( BlockType.QUERY ) );
        block.process( state );
        block = Block.getBlock( Arrays.asList( COMMENT_BLOCK, "Nobody", COMMENT_BLOCK ) );
        expectedException.expect( TestFailureException.class );
        expectedException.expectMessage( containsString( "Query result doesn't contain the string" ) );
        block.process( state );
    }

    @Test
    public void graph()
    {
        engine.execute( "CREATE (n:Person {name:\"Adam\"});" );
        Block block = Block.getBlock( Arrays.asList( "// graph:xyz" ) );
        assertThat( block.type, sameInstance( BlockType.GRAPH ) );
        String output;
        try (Transaction transaction = database.beginTx())
        {
            output = block.process( state );
            transaction.success();
        }
        assertThat(
                output,
                allOf( startsWith( "[\"dot\"" ), containsString( "Adam" ),
                        containsString( "cypherdoc-xyz" ),
                        containsString( ".svg" ), containsString( "neoviz" ) ) );
    }
   
    @Test
    public void graphWithoutId()
    {
        engine.execute( "CREATE (n:Person {name:\"Adam\"});" );
        Block block = Block.getBlock( Arrays.asList( "//graph" ) );
        assertThat( block.type, sameInstance( BlockType.GRAPH ) );
        String output;
        try (Transaction transaction = database.beginTx())
        {
            output = block.process( state );
            transaction.success();
        }
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
                allOf( startsWith( "ifdef::" ), endsWith( "endif::[]"
                                                          + CypherDoc.EOL ),
                        containsString( "cypherdoc-console" ),
                        containsString( "<p" ), containsString( "<simpara" ),
                        containsString( "html" ) ) );
    }

    @Test
    public void text()
    {
        Block block = Block.getBlock( Arrays.asList( "NOTE: just random asciidoc." ) );
        assertThat( block.type, sameInstance( BlockType.TEXT ) );
        String output = block.process( state );
        assertThat( output, equalTo( "NOTE: just random asciidoc."
                                     + CypherDoc.EOL ) );
    }
}
