/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.TestData;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

public class AncestorTestCase implements GraphHolder
{
    @Rule
    public TestData<Map<String,Node>> data = TestData.producedThrough( GraphDescription.createGraphFor( this, true ) );
    private static GraphDatabaseService gdb;

    @BeforeClass
    public static void before()
    {
        gdb = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @AfterClass
    public static void after()
    {
        gdb.shutdown();
    }

    @Test
    @Graph( { "root contains child1", "child1 contains child11",
            "child1 contains child12", "root contains child2",
            "child12 contains child121", "child1 contains child13" } )
    public void test()
    {
        PathExpander expander = PathExpanders.forTypeAndDirection( Rels.contains, Direction.INCOMING );

        List<Node> nodeSet = new ArrayList<>();
        Map<String, Node> graph = data.get();
        nodeSet.add( graph.get( "child1" ) );
        nodeSet.add( graph.get( "root" ) );

        try ( Transaction transaction = gdb.beginTx() )
        {
            Node ancestor = AncestorsUtil.lowestCommonAncestor( nodeSet, expander);
            assertEquals(graph.get( "root" ), ancestor);

            nodeSet.clear();
            nodeSet.add( graph.get( "child12" ) );
            nodeSet.add( graph.get( "child11" ) );
            ancestor = AncestorsUtil.lowestCommonAncestor( nodeSet, expander);
            assertEquals(graph.get( "child1" ), ancestor);

            nodeSet.clear();
            nodeSet.add( graph.get( "child121" ) );
            nodeSet.add( graph.get( "child12" ) );
            ancestor = AncestorsUtil.lowestCommonAncestor( nodeSet, expander);
            assertEquals(graph.get( "child12" ), ancestor);

            nodeSet.clear();
            nodeSet.add( graph.get( "child11" ) );
            nodeSet.add( graph.get( "child13" ) );
            ancestor = AncestorsUtil.lowestCommonAncestor( nodeSet, expander);
            assertEquals(graph.get( "child1" ), ancestor);

            nodeSet.clear();
            nodeSet.add( graph.get( "child2" ) );
            nodeSet.add( graph.get( "child121" ) );
            ancestor = AncestorsUtil.lowestCommonAncestor( nodeSet, expander);
            assertEquals(graph.get( "root" ), ancestor);

            nodeSet.clear();
            nodeSet.add( graph.get( "child11" ) );
            nodeSet.add( graph.get( "child12" ) );
            nodeSet.add( graph.get( "child13" ) );
            ancestor = AncestorsUtil.lowestCommonAncestor( nodeSet, expander);
            assertEquals(graph.get( "child1" ), ancestor);

            nodeSet.clear();
            nodeSet.add( graph.get( "child11" ) );
            nodeSet.add( graph.get( "child12" ) );
            nodeSet.add( graph.get( "child13" ) );
            nodeSet.add( graph.get( "child121" ) );
            ancestor = AncestorsUtil.lowestCommonAncestor( nodeSet, expander);
            assertEquals(graph.get( "child1" ), ancestor);

            nodeSet.clear();
            nodeSet.add( graph.get( "child11" ) );
            nodeSet.add( graph.get( "child12" ) );
            nodeSet.add( graph.get( "child13" ) );
            nodeSet.add( graph.get( "child121" ) );
            nodeSet.add( graph.get( "child2" ) );
            ancestor = AncestorsUtil.lowestCommonAncestor( nodeSet, expander);
            assertEquals(graph.get( "root" ), ancestor);

            nodeSet.clear();
            nodeSet.add( graph.get( "child11" ) );
            nodeSet.add( graph.get( "child12" ) );
            nodeSet.add( graph.get( "child13" ) );
            nodeSet.add( graph.get( "child121" ) );
            nodeSet.add( graph.get( "child12" ) );
            nodeSet.add( graph.get( "root" ) );
            ancestor = AncestorsUtil.lowestCommonAncestor( nodeSet, expander);
            assertEquals(graph.get( "root" ), ancestor);
            transaction.success();
        }
    }

    @Override
    public GraphDatabaseService graphdb()
    {
        return gdb;
    }

    enum Rels implements RelationshipType
    {
        contains
    }
}
