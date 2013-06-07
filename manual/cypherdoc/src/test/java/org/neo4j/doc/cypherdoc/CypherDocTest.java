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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class CypherDocTest
{

    @Test
    public void fullDocumentParsing() throws IOException
    {
        String content = FileUtils.readFileToString( new File(
                "src/test/resources/hello-world.asciidoc" ) );
        List<Block> blocks = CypherDoc.parseBlocks( content );
        List<BlockType> types = new ArrayList<BlockType>();
        for ( Block block : blocks )
        {
            types.add( block.type );
        }
        assertThat( types, equalTo( Arrays.asList( BlockType.TITLE,
                BlockType.TEXT, BlockType.CONSOLE, BlockType.QUERY,
                BlockType.GRAPH, BlockType.TEXT, BlockType.QUERY ) ) );
    }
}
