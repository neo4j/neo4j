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
package org.neo4j.kernel.impl.traversal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.kernel.Traversal;

public class TestPath extends AbstractTestBase
{
    @BeforeClass
    public static void setup()
    {
        createGraph( "A TO B", "B TO C", "C TO D", "D TO E" );
    }
    
    @Test
    public void testPathIterator()
    {
        Path path = Traversal.description().evaluator( Evaluators.atDepth( 4 ) ).traverse(
                node( "A" ) ).iterator().next();
        
        assertPathIsCorrect( path );
    }

    private void assertPathIsCorrect( Path path )
    {
        Node a = node( "A" );
        Relationship to1 = a.getRelationships( Direction.OUTGOING ).iterator().next();
        Node b = to1.getEndNode();
        Relationship to2 = b.getRelationships( Direction.OUTGOING ).iterator().next();
        Node c = to2.getEndNode();
        Relationship to3 = c.getRelationships( Direction.OUTGOING ).iterator().next();
        Node d = to3.getEndNode();
        Relationship to4 = d.getRelationships( Direction.OUTGOING ).iterator().next();
        Node e = to4.getEndNode();
        assertEquals( (Integer) 4, (Integer) path.length() );
        assertEquals( a, path.startNode() );
        assertEquals( e, path.endNode() );
        assertEquals( to4, path.lastRelationship() );
        
        Iterator<PropertyContainer> pathEntities = path.iterator();
        assertEquals( a, pathEntities.next() );
        assertEquals( to1, pathEntities.next() );
        assertEquals( b, pathEntities.next() );
        assertEquals( to2, pathEntities.next() );
        assertEquals( c, pathEntities.next() );
        assertEquals( to3, pathEntities.next() );
        assertEquals( d, pathEntities.next() );
        assertEquals( to4, pathEntities.next() );
        assertEquals( e, pathEntities.next() );
        assertFalse( pathEntities.hasNext() );
        
        Iterator<Node> nodes = path.nodes().iterator();
        assertEquals( a, nodes.next() );
        assertEquals( b, nodes.next() );
        assertEquals( c, nodes.next() );
        assertEquals( d, nodes.next() );
        assertEquals( e, nodes.next() );
        assertFalse( nodes.hasNext() );
        
        Iterator<Relationship> relationships = path.relationships().iterator();
        assertEquals( to1, relationships.next() );
        assertEquals( to2, relationships.next() );
        assertEquals( to3, relationships.next() );
        assertEquals( to4, relationships.next() );
        assertFalse( relationships.hasNext() );
    }
}
