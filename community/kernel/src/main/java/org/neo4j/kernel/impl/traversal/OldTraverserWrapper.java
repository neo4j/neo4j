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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.OrderedByTypeExpander;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

public class OldTraverserWrapper
{
    private static class TraverserImpl implements org.neo4j.graphdb.Traverser,
            Iterator<Node>
    {
        private TraversalPosition currentPos;
        private Iterator<Path> iter;
        private int count;

        public TraversalPosition currentPosition()
        {
            return currentPos;
        }

        public int numberOfNodesReturned()
        {
            return count;
        }

        public Collection<Node> getAllNodes()
        {
            List<Node> result = new ArrayList<Node>();
            for ( Node node : this )
            {
                result.add( node );
            }
            return result;
        }

        public Iterator<Node> iterator()
        {
            return this;
        }

        public boolean hasNext()
        {
            return iter.hasNext();
        }

        public Node next()
        {
            currentPos = new PositionImpl( this, iter.next() );
            count++;
            return currentPos.currentNode();
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class PositionImpl implements TraversalPosition
    {
        private final Path position;
        private final int count;

        PositionImpl( TraverserImpl traverser, Path position )
        {
            this.position = position;
            this.count = traverser.numberOfNodesReturned();
        }

        public Node currentNode()
        {
            return position.endNode();
        }

        public int depth()
        {
            return position.length();
        }

        public boolean isStartNode()
        {
            return position.length() == 0;
        }

        public boolean notStartNode()
        {
            return !isStartNode();
        }

        public Relationship lastRelationshipTraversed()
        {
            return position.lastRelationship();
        }

        public Node previousNode()
        {
            return position.lastRelationship().getOtherNode( position.endNode() );
        }

        public int returnedNodesCount()
        {
            return count;
        }

    }

    private static void assertNotNull( Object object, String message )
    {
        if ( object == null )
        {
            throw new IllegalArgumentException( "Null " + message );
        }
    }

    private static final TraversalDescription BASE_DESCRIPTION =
            Traversal.traversal().uniqueness( Uniqueness.NODE_GLOBAL );

    public static org.neo4j.graphdb.Traverser traverse( Node node, Order traversalOrder,
            StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator,
            Object... relationshipTypesAndDirections )
    {
        assertNotNull( traversalOrder, "order" );
        assertNotNull( stopEvaluator, "stop evaluator" );
        assertNotNull( returnableEvaluator, "returnable evaluator" );

        if ( relationshipTypesAndDirections.length % 2 != 0
             || relationshipTypesAndDirections.length == 0 )
        {
            throw new IllegalArgumentException();
        }
        TraverserImpl result = new TraverserImpl();
        TraversalDescription description = traversal( result, traversalOrder,
                stopEvaluator, returnableEvaluator );
        description = description.expand( toExpander( relationshipTypesAndDirections ) );
        result.iter = description.traverse( node ).iterator();
        return result;
    }

    private static RelationshipExpander toExpander(
            Object[] relationshipTypesAndDirections )
    {
        Stack<Object[]> entries = new Stack<Object[]>();
        for ( int i = 0; i < relationshipTypesAndDirections.length; i += 2 )
        {
            Object relType = relationshipTypesAndDirections[i];
            if ( relType == null )
            {
                throw new IllegalArgumentException(
                        "Null relationship type at " + i );
            }
            if ( !(relType instanceof RelationshipType) )
            {
                throw new IllegalArgumentException(
                    "Expected RelationshipType at var args pos " + i
                        + ", found " + relType );
            }
            Object direction = relationshipTypesAndDirections[i+1];
            if ( direction == null )
            {
                throw new IllegalArgumentException(
                        "Null direction at " + (i+1) );
            }
            if ( !(direction instanceof Direction) )
            {
                throw new IllegalArgumentException(
                    "Expected Direction at var args pos " + (i+1)
                        + ", found " + direction );
            }
            entries.push( new Object[] { relType, direction } );
        }

        OrderedByTypeExpander expander = new OrderedByTypeExpander();
        while ( !entries.isEmpty() )
        {
            Object[] entry = entries.pop();
            expander = (OrderedByTypeExpander) expander.add(
                    (RelationshipType) entry[0], (Direction) entry[1] );
        }
        return expander;
    }

    private static TraversalDescription traversal( TraverserImpl traverser,
            Order order, StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator )
    {
        TraversalDescription description = BASE_DESCRIPTION;
        switch ( order )
        {
        case BREADTH_FIRST:
            description = description.breadthFirst();
            break;
        case DEPTH_FIRST:
            description = description.depthFirst();
            break;

        default:
            throw new IllegalArgumentException( "Onsupported traversal order: "
                                                + order );
        }

        description = description.evaluator( new Pruner( traverser, stopEvaluator ) );
        description = description.evaluator( new Filter( traverser, returnableEvaluator ) );

        return description;
    }

    private static class Pruner implements Evaluator
    {
        private final TraverserImpl traverser;
        private final StopEvaluator evaluator;

        Pruner( TraverserImpl traverser, StopEvaluator stopEvaluator )
        {
            this.traverser = traverser;
            this.evaluator = stopEvaluator;
        }
        
        @Override
        public Evaluation evaluate( Path path )
        {
            return Evaluation.ofContinues( !evaluator.isStopNode( new PositionImpl( traverser, path ) ) );
        }
    }

    private static class Filter implements Evaluator
    {
        private final TraverserImpl traverser;
        private final ReturnableEvaluator evaluator;

        Filter( TraverserImpl traverser, ReturnableEvaluator returnableEvaluator )
        {
            this.traverser = traverser;
            this.evaluator = returnableEvaluator;
        }

        @Override
        public Evaluation evaluate( Path path )
        {
            return Evaluation.ofIncludes( evaluator.isReturnableNode(
                    new PositionImpl( traverser, path ) ) );
        }
    }
}
