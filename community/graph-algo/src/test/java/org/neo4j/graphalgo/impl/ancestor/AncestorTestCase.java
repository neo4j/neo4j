/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.graphalgo.impl.ancestor;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestData;

public class AncestorTestCase implements GraphHolder
{

    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor(
            this, true ) );
    private static GraphDatabaseService gdb;

    @Test
    @Graph( { "root contains child1", "child1 contains child11",
            "child1 contains child12", "root contains child2" } )
    public void test()
    {
        List<Node> nodeSet = new ArrayList<Node>();
        Map<String, Node> graph = data.get();
        nodeSet.add( graph.get( "child12" ) );
        nodeSet.add( graph.get( "child11" ) );
        Node ancestor = AncestorsUtil.lowestCommonAncestor( nodeSet, Rels.contains,
                Direction.INCOMING );
        assertEquals(graph.get( "child1" ), ancestor);
    }

    @Override
    public GraphDatabaseService graphdb()
    {
        return gdb;
    }

    @BeforeClass
    public static void before()
    {
        gdb = new ImpermanentGraphDatabase();
    }
    @AfterClass
    public static void after()
    {
        gdb.shutdown();
    }
    enum Rels implements RelationshipType
    {
        contains;
    }
}
