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
package org.neo4j.shell.cypher;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.MapUtil.map;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.shell.impl.CollectingOutput;
import org.neo4j.shell.kernel.apps.cypher.ResultPrinter;
import org.neo4j.test.ImpermanentGraphDatabase;

public class ResultPrinterTest
{

    private final ResultPrinter resultPrinter = new ResultPrinter();
    private static GraphDatabaseService db = new ImpermanentGraphDatabase();

    private static final String LN = System.getProperty( "line.separator" );

    @Before
    public void setUp() throws Exception
    {

    }

    @Test
    public void testEmptyResult() throws Exception
    {
        List<Map<String, Object>> rows = Collections.emptyList();

        final CollectingOutput output = new CollectingOutput();
        resultPrinter.outputResults( asList( "foo" ), rows, 0, null, output );
        assertEquals(
                "+-----+"+LN+
                        "| foo |"+LN+
                        "+-----+"+LN+
                        "+-----+"+LN+
                        "0 rows"+LN+
                        "0 ms"+LN+"",
                output.asString() );
    }

    @Test
    public void testSingleNode() throws Exception
    {
        final Node n = db.createNode();
        List<Map<String, Object>> rows = asList( map( "foo", n ) );

        final CollectingOutput output = new CollectingOutput();
        resultPrinter.outputResults( asList( "foo" ), rows, 1, null, output );
        String out = output.asString();
        assertEquals(
            "+-----------+"+LN+
            "| foo       |"+LN+
            "+-----------+"+LN+
            "| Node[" + n.getId() + "]{} |"+LN+
            "+-----------+"+LN+
            "1 row"+LN+
            "1 ms"+LN+"".toCharArray(), out.toCharArray() );
    }

    @Test
    public void testSingleNodeWithProperties() throws Exception
    {
        db.beginTx();
        final Node n = db.createNode();
        n.setProperty( "last_checked", 1360610542463L );
        n.setProperty( "foo", new int[]{1, 2, 3} );

        List<Map<String, Object>> rows = asList( map( "n", n ), map( "n", n ) );

        final CollectingOutput output = new CollectingOutput();
        resultPrinter.outputResults( asList( "n" ), rows, 100, null, output );
        assertEquals(
                "+-------------------------------------------------+"+LN+
                        "| n                                               |"+LN+
                        "+-------------------------------------------------+"+LN+
                        "| Node[" + n.getId() + "]{last_checked:1360610542463,foo:[1,2,3]} |"+LN+
                        "| Node[" + n.getId() + "]{last_checked:1360610542463,foo:[1,2,3]} |"+LN+
                        "+-------------------------------------------------+"+LN+
                        "2 rows"+LN+
                        "100 ms"+LN+""
                ,
                output.asString() );
    }

    @Test
    public void testComplexResult() throws Exception
    {
        db.beginTx();
        final Node n0 = db.createNode();
        final Node n1 = db.createNode();
        final Relationship r = n0.createRelationshipTo( n1, DynamicRelationshipType.withName( "RELATED_TO" ) );
        final Path p = new PathImpl.Builder( n0 ).push( r ).build();

        List<Map<String, Object>> rows = asList(
                map( "n", n0, "m", n1, "r", r, "p", p
                ) );

        final CollectingOutput output = new CollectingOutput();
        resultPrinter.outputResults( asList( "n", "m", "r", "p" ), rows, 1, null, output );
        final String text = output.asString();

        assertEquals(
                "+-------------------------------------------------------------------------------------+"+LN+
                        "| n         | m         | r                 | p                                       |"+LN+
                        "+-------------------------------------------------------------------------------------+"+LN+
                        "| Node[" + n0.getId() + "]{} | Node[" + n1.getId() + "]{} | :RELATED_TO[" + r.getId() + "] {} | [Node[" + n0.getId() + "]{},:RELATED_TO[" + r.getId() + "] {},Node[" + n1.getId() + "]{}] |"+LN+
                        "+-------------------------------------------------------------------------------------+"+LN+
                        "1 row"+LN+
                        "1 ms"+LN+""
                ,
                text );
    }

    @Test
    public void testSingleRow() throws Exception
    {
        List<Map<String, Object>> rows = asList( singletonMap( "foo", (Object) "barbar" ) );
        final CollectingOutput output = new CollectingOutput();
        resultPrinter.outputResults( asList( "foo" ), rows, 0, null, output );
        System.out.println( "text = " + output.asString() );
    }
}
