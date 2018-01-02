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

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CircularGraphTest extends TraversalTestBase
{
    @Before
    public void createTheGraph()
    {
        createGraph( "1 TO 2", "2 TO 3", "3 TO 1" );
    }

    @Test
    public void testCircularBug()
    {
        final long timestamp = 3;
        try ( Transaction tx = beginTx() )
        {
            getNodeWithName( "2" ).setProperty( "timestamp", 1L );
            getNodeWithName( "3" ).setProperty( "timestamp", 2L );
            tx.success();
        }

        try ( Transaction tx2 = beginTx() )
        {
            final RelationshipType type = DynamicRelationshipType.withName( "TO" );
            Traverser t = node( "1" ).traverse( Order.DEPTH_FIRST, new StopEvaluator()
                    {
                        public boolean isStopNode( TraversalPosition position )
                        {
                            Relationship last = position.lastRelationshipTraversed();
                            if ( last != null && last.isType( type ) )
                            {
                                Node node = position.currentNode();
                                long currentTime = (Long) node.getProperty( "timestamp" );
                                return currentTime >= timestamp;
                            }
                            return false;
                        }
                    }, new ReturnableEvaluator()
                    {
                        public boolean isReturnableNode( TraversalPosition position )
                        {
                            Relationship last = position.lastRelationshipTraversed();
                            if ( last != null && last.isType( type ) )
                            {
                                return true;
                            }
                            return false;
                        }
                    }, type, Direction.OUTGOING
            );
            Iterator<Node> nodes = t.iterator();

            assertEquals( "2", nodes.next().getProperty( "name" ) );
            assertEquals( "3", nodes.next().getProperty( "name" ) );
            assertFalse( nodes.hasNext() );
        }
    }
}
