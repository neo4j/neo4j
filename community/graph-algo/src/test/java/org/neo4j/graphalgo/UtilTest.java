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
package org.neo4j.graphalgo;

import common.Neo4jAlgoTestCase;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphalgo.impl.shortestpath.Util;
import org.neo4j.graphalgo.impl.shortestpath.Util.PathCounter;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import static org.junit.Assert.assertTrue;

public class UtilTest extends Neo4jAlgoTestCase
{
    @Test
    public void testPathCounter()
    {
        // Nodes
        Node a = graphDb.createNode();
        Node b = graphDb.createNode();
        Node c = graphDb.createNode();
        Node d = graphDb.createNode();
        Node e = graphDb.createNode();
        Node f = graphDb.createNode();
        // Predecessor lists
        List<Relationship> ap = new LinkedList<>();
        List<Relationship> bp = new LinkedList<>();
        List<Relationship> cp = new LinkedList<>();
        List<Relationship> dp = new LinkedList<>();
        List<Relationship> ep = new LinkedList<>();
        List<Relationship> fp = new LinkedList<>();
        // Predecessor map
        Map<Node,List<Relationship>> predecessors = new HashMap<>();
        predecessors.put( a, ap );
        predecessors.put( b, bp );
        predecessors.put( c, cp );
        predecessors.put( d, dp );
        predecessors.put( e, ep );
        predecessors.put( f, fp );
        // Add relations
        fp.add( f.createRelationshipTo( c, MyRelTypes.R1 ) );
        fp.add( f.createRelationshipTo( e, MyRelTypes.R1 ) );
        ep.add( e.createRelationshipTo( b, MyRelTypes.R1 ) );
        ep.add( e.createRelationshipTo( d, MyRelTypes.R1 ) );
        dp.add( d.createRelationshipTo( a, MyRelTypes.R1 ) );
        cp.add( c.createRelationshipTo( b, MyRelTypes.R1 ) );
        bp.add( b.createRelationshipTo( a, MyRelTypes.R1 ) );
        // Count
        PathCounter counter = new Util.PathCounter( predecessors );
        assertTrue( counter.getNumberOfPathsToNode( a ) == 1 );
        assertTrue( counter.getNumberOfPathsToNode( b ) == 1 );
        assertTrue( counter.getNumberOfPathsToNode( c ) == 1 );
        assertTrue( counter.getNumberOfPathsToNode( d ) == 1 );
        assertTrue( counter.getNumberOfPathsToNode( e ) == 2 );
        assertTrue( counter.getNumberOfPathsToNode( f ) == 3 );
        // Reverse
        counter = new Util.PathCounter( Util.reversedPredecessors( predecessors ) );
        assertTrue( counter.getNumberOfPathsToNode( a ) == 3 );
        assertTrue( counter.getNumberOfPathsToNode( b ) == 2 );
        assertTrue( counter.getNumberOfPathsToNode( c ) == 1 );
        assertTrue( counter.getNumberOfPathsToNode( d ) == 1 );
        assertTrue( counter.getNumberOfPathsToNode( e ) == 1 );
        assertTrue( counter.getNumberOfPathsToNode( f ) == 1 );
    }
}
