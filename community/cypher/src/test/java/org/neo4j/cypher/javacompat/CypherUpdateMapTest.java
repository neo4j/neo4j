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
package org.neo4j.cypher.javacompat;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.test.ImpermanentGraphDatabase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.helpers.collection.MapUtil.map;

public class CypherUpdateMapTest
{

    private ExecutionEngine engine;
    private ImpermanentGraphDatabase gdb;

    @Test
    public void updateNodeByMapParameter()
    {
        engine.execute(
                "START n=node(0) SET n = {data} RETURN n" ,
                map( "data",
                        map("key1", "value1", "key2", 1234)
                )
        );

        Node node1 = gdb.getNodeById(0);

        assertEquals("value1", node1.getProperty("key1"));
        assertEquals(1234, node1.getProperty("key2"));

        engine.execute(
                "START n=node(0) SET n = {data} RETURN n",
                map( "data",
                        map("key1", null, "key3", 5678)
                )
        );

        Node node2 = gdb.getNodeById(0);

        assertFalse(node2.hasProperty("key1"));
        assertFalse(node2.hasProperty("key2"));
        assertEquals(5678, node2.getProperty("key3"));

    }

    @Before
    public void setup()
    {
        gdb = new ImpermanentGraphDatabase();
        engine = new ExecutionEngine(gdb);
    }
}