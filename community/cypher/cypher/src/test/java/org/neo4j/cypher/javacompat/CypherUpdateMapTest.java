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
package org.neo4j.cypher.javacompat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;
import static org.neo4j.helpers.collection.MapUtil.map;

public class CypherUpdateMapTest
{

    private ExecutionEngine engine;
    private GraphDatabaseService gdb;

    @Test
    public void updateNodeByMapParameter()
    {
        engine.execute(
                "CREATE (n:Reference) SET n = {data} RETURN n" ,
                map( "data",
                        map("key1", "value1", "key2", 1234)
                )
        );

        Node node1 = getNodeByIdInTx( 0 );

        assertThat( node1, inTx( gdb, hasProperty( "key1" ).withValue( "value1" ) ) );
        assertThat( node1, inTx( gdb, hasProperty( "key2" ).withValue( 1234 ) ) );

        engine.execute(
                "MATCH (n:Reference) SET n = {data} RETURN n",
                map( "data",
                        map("key1", null, "key3", 5678)
                )
        );

        Node node2 = getNodeByIdInTx( 0 );

        assertThat( node2, inTx( gdb, not( hasProperty( "key1" ) ) ) );
        assertThat( node2, inTx( gdb, not( hasProperty( "key2" ) ) ) );
        assertThat( node2, inTx( gdb, hasProperty( "key3" ).withValue(5678) ) );
    }

    private Node getNodeByIdInTx( int nodeId )
    {
        try ( Transaction ignored = gdb.beginTx(); )
        {
            return gdb.getNodeById( nodeId );
        }
    }

    @Before
    public void setup()
    {
        gdb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        engine = new ExecutionEngine(gdb);
    }

    @After
    public void cleanup()
    {
        gdb.shutdown();
    }
}
