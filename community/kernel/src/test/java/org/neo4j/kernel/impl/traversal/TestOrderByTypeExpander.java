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
package org.neo4j.kernel.impl.traversal;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.OrderedByTypeExpander;

import static org.junit.Assert.*;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.kernel.Traversal.traversal;

public class TestOrderByTypeExpander extends TraversalTestBase
{
    private final RelationshipType next = withName( "NEXT" );
    private final RelationshipType firstComment = withName( "FIRST_COMMENT" );
    private final RelationshipType comment = withName( "COMMENT" );
    
    @Before
    public void setup()
    {
        /**
         *                          (A1)-NEXT->(A2)-NEXT->(A3)
         *                        /             |              \
         *            FIRST_COMMENT      FIRST_COMMENT        FIRST_COMMENT
         *             /                        |                    \
         *            v                         v                     v
         *          (C1)                       (C4)                  (C7)
         *           |                          |                     |
         *        COMMENT                    COMMENT               COMMENT
         *           |                          |                     |
         *           v                          v                     v
         *          (C2)                       (C5)                  (C8)
         *           |                          |                     |
         *        COMMENT                    COMMENT               COMMENT
         *           |                          |                     |
         *           v                          v                     v
         *          (C3)                       (C6)                  (C9)
         */
        createGraph( "A1 NEXT A2", "A2 NEXT A3",
                "A1 FIRST_COMMENT C1", "C1 COMMENT C2", "C2 COMMENT C3",
                "A2 FIRST_COMMENT C4", "C4 COMMENT C5", "C5 COMMENT C6",
                "A3 FIRST_COMMENT C7", "C7 COMMENT C8", "C8 COMMENT C9" );
    }
    
    @Test
    public void makeSureNodesAreTraversedInCorrectOrder()
    {
        RelationshipExpander expander =
            new OrderedByTypeExpander().add( firstComment ).add( comment ).add( next );
        Iterator<Node> itr = traversal().depthFirst().expand(
                expander ).traverse( node( "A1" ) ).nodes().iterator();
        assertOrder( itr, "A1", "C1", "C2", "C3", "A2", "C4", "C5", "C6", "A3", "C7", "C8", "C9" );

        expander = new OrderedByTypeExpander().add( next ).add( firstComment ).add( comment );
        itr = traversal().depthFirst().expand(
                expander ).traverse( node( "A1" ) ).nodes().iterator();
        assertOrder( itr, "A1", "A2", "A3", "C7", "C8", "C9", "C4", "C5", "C6", "C1", "C2", "C3" );
    }
    
    @Test
    public void evenDifferentDirectionsKeepsOrder() throws Exception
    {
        RelationshipExpander expander = new OrderedByTypeExpander()
                .add( next, INCOMING )
                .add( firstComment )
                .add( comment )
                .add( next, OUTGOING );
        Iterator<Node> itr = traversal().depthFirst().expand( expander ).traverse( node( "A2" ) ).nodes().iterator();
        assertOrder( itr, "A2", "A1", "C1", "C2", "C3", "C4", "C5", "C6", "A3", "C7", "C8", "C9" );
    }
    
    private void assertOrder( Iterator<Node> itr, String... names )
    {
        try ( Transaction tx = beginTx() )
        {
            for ( String name : names )
            {
                Node node = itr.next();
                assertEquals( "expected " + name + ", was " + node.getProperty( "name" ),
                        getNodeWithName( name ), node );
            }
            assertFalse( itr.hasNext() );
            tx.success();
        }
    }
}
