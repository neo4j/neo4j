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
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;

public class CypherDocTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void fullDocumentBlockParsing() throws IOException
    {
        String content = FileUtils.readFileToString( resourceFile( "/hello-world.asciidoc" ) );
        List<Block> blocks = CypherDoc.parseBlocks( content );
        List<BlockType> types = new ArrayList<BlockType>();
        for ( Block block : blocks )
        {
            types.add( block.type );
        }
        assertThat( types, equalTo( Arrays.asList( BlockType.TITLE, BlockType.TEXT, BlockType.HIDE,
                BlockType.SETUP, BlockType.CYPHER, BlockType.QUERYTEST, BlockType.TABLE, BlockType.GRAPH, BlockType.TEXT,
                BlockType.OUTPUT, BlockType.PARAMETERS, BlockType.CYPHER, BlockType.QUERYTEST, BlockType.PROFILE,
                BlockType.GRAPH_RESULT, BlockType.SQL, BlockType.SQL_TABLE, BlockType.TEXT ) ) );
    }

    @Test
    public void notEnoughContentBlockParsing()
    {
        expectedException.expect( IllegalArgumentException.class );
        CypherDoc.parseBlocks( "x\ny\n" );
    }

    @Test
    public void shouldEmitProfileOnTestFailure() throws Exception
    {
        // given
        String content = FileUtils.readFileToString( resourceFile( "/failing-query.asciidoc" ) );

        // when
        try
        {
            CypherDoc.parse( content, null, "http://url/" );
            fail( "expected exception" );
        }
        // then
        catch ( TestFailureException e )
        {
            String failure = e.toString();
            assertThat( failure, containsString( "Query result doesn't contain the string '1 row'." ) );
            assertThat( failure, containsString( "Query:" + CypherDoc.EOL + '\t' + CypherDoc.indent( e.result.query ) ) );
            assertThat( failure, containsString( "Result:" + CypherDoc.EOL + '\t' + CypherDoc.indent( e.result.text ) ) );
            assertThat( failure, containsString( "Profile:" + CypherDoc.EOL + '\t' + CypherDoc.indent( e.result.profile ) ) );
        }
    }

    @Test
    public void fullDocumentParsing() throws IOException
    {
        String content = FileUtils.readFileToString( resourceFile( "/hello-world.asciidoc" ) );
        String output = CypherDoc.parse( content, null, "http://url/" );
        assertThat(
                output,
                allOf( containsString( "[[cypherdoc-hello-world]]" ),
                        containsString( "<p class=\"cypherdoc-console\"></p>" ), containsString( "[source,cypher]" ),
                        containsString( "[queryresult]" ), containsString( "{Person|name = \\'Adam\\'\\l}" ),
                        containsString( "= Hello World =" ) ) );
        assertThat(
                output,
                allOf( containsString( "<span class=\"hide-query\"></span>" ),
                        containsString( "<span class=\"setup-query\"></span>" ),
                        containsString( "<span class=\"query-output\"></span>" ),
                        containsString( "<simpara role=\"query-output\"></simpara>" ) ) );

        assertThat( output, containsString( "cypherdoc-result" ) );
    }

    @Test
    public void test_both_against_cypher_and_sql() throws IOException
    {
        String content = FileUtils.readFileToString( resourceFile( "/tests-with-sql.asciidoc" ) );
        String output = CypherDoc.parse( content, null, "http://url/" );
    }

    @Test
    public void test_profiling_output() throws IOException
    {
        String content = FileUtils.readFileToString( resourceFile( "/profiling-test.asciidoc" ) );
        String output = CypherDoc.parse( content, null, "http://url/" );
    }

    @Test
    public void test_rewindable_results() throws IOException
    {
        String content = FileUtils.readFileToString( resourceFile( "/patterns-in-practice.adoc" ) );
        String output = CypherDoc.parse( content, null, "http://url/" );
    }

    private File resourceFile( String resource ) throws IOException
    {
        try
        {
            return new File( getClass().getResource( resource ).toURI() );
        }
        catch ( NullPointerException | URISyntaxException e )
        {
            throw new IOException( "Could not find resource: " + resource, e );
        }
    }
}
