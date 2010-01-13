/*
 * Copyright 2008 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphalgo.shortestpath.Util;
import org.neo4j.graphalgo.shortestpath.Util.PathCounter;
import org.neo4j.graphalgo.testUtil.NeoAlgoTestCase;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class UtilTest extends NeoAlgoTestCase
{
    public UtilTest( String name )
    {
        super( name );
    }

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
        List<Relationship> ap = new LinkedList<Relationship>();
        List<Relationship> bp = new LinkedList<Relationship>();
        List<Relationship> cp = new LinkedList<Relationship>();
        List<Relationship> dp = new LinkedList<Relationship>();
        List<Relationship> ep = new LinkedList<Relationship>();
        List<Relationship> fp = new LinkedList<Relationship>();
        // Predecessor map
        Map<Node,List<Relationship>> predecessors = new HashMap<Node,List<Relationship>>();
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
        counter = new Util.PathCounter( Util.reversedPredecessors( predecessors ));
        assertTrue( counter.getNumberOfPathsToNode( a ) == 3 );
        assertTrue( counter.getNumberOfPathsToNode( b ) == 2 );
        assertTrue( counter.getNumberOfPathsToNode( c ) == 1 );
        assertTrue( counter.getNumberOfPathsToNode( d ) == 1 );
        assertTrue( counter.getNumberOfPathsToNode( e ) == 1 );
        assertTrue( counter.getNumberOfPathsToNode( f ) == 1 );
    }
}
