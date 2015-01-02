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
package org.neo4j.cypher.example;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.neo4j.test.JavaDocsGenerator;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

public class JavaQueryDocTest
{
    @Test
    public void test()
    {
        JavaDocsGenerator gen = new JavaDocsGenerator( "java-cypher-queries", "dev/java" );

        JavaQuery jq = new JavaQuery();
        jq.run();
        assertTrue( jq.columnsString.contains( "n," ) );
        assertTrue( jq.columnsString.contains( "n.name" ) );
        assertTrue( jq.resultString.contains( "Node[" ) );
        assertTrue( jq.resultString.contains( "name" ) );
        assertTrue( jq.resultString.contains( "my" ) );
        assertTrue( jq.resultString.contains( "1 row" ) );
        assertTrue( jq.nodeResult.contains( "Node[" ) );
        assertTrue( jq.nodeResult.contains( "my" ) );
        assertTrue( jq.rows.contains( "n.name: my node; n: Node[" ) );
        assertTrue( jq.rows.contains( "];" ) );

        gen.saveToFile( "result", AsciidocHelper.createOutputSnippet( jq.resultString ) );
        gen.saveToFile( "columns", AsciidocHelper.createOutputSnippet( jq.columnsString ) );
        gen.saveToFile( "node", AsciidocHelper.createOutputSnippet( jq.nodeResult ) );
        gen.saveToFile( "rows", AsciidocHelper.createOutputSnippet( jq.rows ) );
    }
}
