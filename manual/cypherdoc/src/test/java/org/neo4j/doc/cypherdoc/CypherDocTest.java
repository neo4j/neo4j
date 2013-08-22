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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.allOf;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CypherDocTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void fullDocumentBlockParsing() throws IOException
    {
        String content = FileUtils.readFileToString( new File(
                "src/test/resources/hello-world.asciidoc" ) );
        List<Block> blocks = CypherDoc.parseBlocks( content );
        List<BlockType> types = new ArrayList<BlockType>();
        for ( Block block : blocks )
        {
            types.add( block.type );
        }
        assertThat( types, equalTo( Arrays.asList( BlockType.TITLE, BlockType.TEXT, BlockType.HIDE,
                BlockType.SETUP, BlockType.QUERY, BlockType.TEST, BlockType.TABLE, BlockType.GRAPH, BlockType.TEXT,
                BlockType.OUTPUT, BlockType.QUERY, BlockType.TEST ) ) );
    }

    @Test
    public void toLittleContentBlockParsing()
    {
        expectedException.expect( IllegalArgumentException.class );
        CypherDoc.parseBlocks( "x\ny\n" );
    }

    @Test
    public void fullDocumentParsing() throws IOException
    {
        String content = FileUtils.readFileToString( new File( "src/test/resources/hello-world.asciidoc" ) );
        String output = CypherDoc.parse( content );
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
    }
}
