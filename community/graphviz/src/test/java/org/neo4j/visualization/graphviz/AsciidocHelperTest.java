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
package org.neo4j.visualization.graphviz;

import org.junit.Test;

import org.neo4j.visualization.asciidoc.AsciidocHelper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AsciidocHelperTest
{

    @Test
    public void test()
    {
        String cypher = "start n=node(0) " +
        		"match " +
        		"x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, " +
                "x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, " +
                "x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n, x-n " +
                "return n, x";
        
        String snippet = AsciidocHelper.createCypherSnippet( cypher );
        assertTrue(snippet.contains( "n,\n" ));
    }

    @Test
    public void shouldBreakAtTheRightSpotWithOnMatch()
    {
        // given
        String cypher = "merge (a)\non match set a.foo = 2";

        //when
        String snippet = AsciidocHelper.createCypherSnippet(cypher);

        //then
        assertEquals(
                "[source,cypher]\n" +
                "----\n" +
                "MERGE (a)\n" +
                "ON MATCH SET a.foo = 2\n" +
                "----\n", snippet);
    }

    @Test
    public void testUpcasingLabels() {
        String queryString  = "create n label :Person {} on tail";
        String snippet = AsciidocHelper.createCypherSnippet( queryString );

        assertTrue( snippet.contains( "LABEL" ) );
        assertTrue( snippet.contains( "ON" ) );
        assertFalse( snippet.contains( ":PersON" ) );
    }
}
