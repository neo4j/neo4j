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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static junit.framework.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.map;

public class ResultPrinterTest
{

    private final ResultPrinter resultPrinter = new ResultPrinter();
    private static GraphDatabaseService db = new ImpermanentGraphDatabase();

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
                "+-----+\n" +
                        "| foo |\n" +
                        "+-----+\n" +
                        "+-----+\n" +
                        "0 rows\n" +
                        "0 ms\n",
                output.asString() );
    }

    @Test
    public void testSingleNode() throws Exception
    {
        final Node n = db.createNode();
        List<Map<String, Object>> rows = asList( map( "foo", n ) );

        final CollectingOutput output = new CollectingOutput();
        resultPrinter.outputResults( asList( "foo" ), rows, 1, null, output );
        assertEquals(
                "+-----------+\n" +
                        "| foo       |\n" +
                        "+-----------+\n" +
                        "| Node[" + n.getId() + "]{} |\n" +
                        "+-----------+\n" +
                        "1 row\n" +
                        "1 ms\n",
                output.asString() );
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
                "+-------------------------------------------------+\n" +
                        "| n                                               |\n" +
                        "+-------------------------------------------------+\n" +
                        "| Node[" + n.getId() + "]{last_checked:1360610542463,foo:[1,2,3]} |\n" +
                        "| Node[" + n.getId() + "]{last_checked:1360610542463,foo:[1,2,3]} |\n" +
                        "+-------------------------------------------------+\n" +
                        "2 rows\n" +
                        "100 ms\n"
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
        System.out.println( text );
        assertEquals(
                "+-------------------------------------------------------------------------------------+\n" +
                        "| n         | m         | r                 | p                                       |\n" +
                        "+-------------------------------------------------------------------------------------+\n" +
                        "| Node[" + n0.getId() + "]{} | Node[" + n1.getId() + "]{} | :RELATED_TO[" + r.getId() + "] {} | [Node[" + n0.getId() + "]{},:RELATED_TO[" + r.getId() + "] {},Node[" + n1.getId() + "]{}] |\n" +
                        "+-------------------------------------------------------------------------------------+\n" +
                        "1 row\n" +
                        "1 ms\n"
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
