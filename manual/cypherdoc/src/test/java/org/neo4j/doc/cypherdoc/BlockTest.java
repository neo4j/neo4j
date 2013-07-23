/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.Matchers.containsString;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

public class BlockTest
{
    private GraphDatabaseService database;
    private ExecutionEngine engine;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup()
    {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        engine = new ExecutionEngine( database );
        CypherDoc.removeReferenceNode( database );
    }

    @After
    public void tearDown()
    {
        database.shutdown();
    }

    @Test
    @SuppressWarnings( "deprecation" )
    public void noReferenceNode()
    {
        expectedException.expect( NotFoundException.class );

        Transaction transaction = database.beginTx();
        try
        {
            database.getReferenceNode();
        }
        finally
        {
            transaction.finish();
        }
    }

    @Test
    public void oneLineTitle()
    {
        Block block = Block.getBlock( Arrays.asList( "= Title here =" ) );
        assertThat( block.type, sameInstance( BlockType.TITLE ) );
        String output = block.process( engine, database );
        assertThat( output, containsString( "[[cypherdoc-title-here]]" ) );
        assertThat( output, containsString( "= Title here =" ) );
    }

    @Test
    public void twoLineTitle()
    {
        Block block = Block.getBlock( Arrays.asList( "Title here", "==========" ) );
        assertThat( block.type, sameInstance( BlockType.TITLE ) );
        String output = block.process( engine, database );
        assertThat( output, containsString( "[[cypherdoc-title-here]]" ) );
        assertThat( output, containsString( "= Title here =" ) );
    }

    @Test
    public void queryWithTestFailure()
    {
        Block block = Block.getBlock( Arrays.asList(
                "[source, cypher, includeresult]", "----",
                "CREATE (n:Person {name:\"Adam\"})", "RETURN n;", "----",
                "Nobody" ) );
        assertThat( block.type, sameInstance( BlockType.QUERY ) );
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( containsString( "Query result doesn't contain the string" ) );
        block.process( engine, database );
    }

    @Test
    public void queryWithResult()
    {
        Block block = Block.getBlock( Arrays.asList(
                "[source, cypher, includeresult]", "----",
                "CREATE (n:Person {name:\"Ad\" + \"am\"})", "RETURN n;",
                "----", "Adam" ) );
        String output = block.process( engine, database );
        assertThat( output, containsString( "Adam" ) );
    }

    @Test
    public void queryWithoutResult()
    {
        Block block = Block.getBlock( Arrays.asList( "[source, cypher]",
                "----", "CREATE (n:Person {name:\"Ad\" + \"am\"})",
                "RETURN n;", "----", "Adam" ) );
        String output = block.process( engine, database );
        assertThat( output, not( containsString( "Adam" ) ) );
    }

    @Test
    public void graph()
    {
        engine.execute( "CREATE (n:Person {name:\"Adam\"});" );
        Block block = Block.getBlock( Arrays.asList( "// graph:xyz" ) );
        assertThat( block.type, sameInstance( BlockType.GRAPH ) );
        Transaction transaction = database.beginTx();
        String output;
        try
        {
            output = block.process( engine, database );
        }
        finally
        {
            transaction.finish();
        }
        assertThat(
                output,
                allOf( startsWith( "[\"dot\"" ), containsString( "Adam" ),
                        containsString( "cypherdoc-xyz" ),
                        containsString( ".svg" ), containsString( "neoviz" ) ) );
    }

    @Test
    public void console()
    {
        Block block = Block.getBlock( Arrays.asList( "// console" ) );
        assertThat( block.type, sameInstance( BlockType.CONSOLE ) );
        String output = block.process( engine, database );
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
        String output = block.process( engine, database );
        assertThat( output, equalTo( "NOTE: just random asciidoc."
                                     + CypherDoc.EOL ) );
    }
}
