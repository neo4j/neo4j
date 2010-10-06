/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.remote;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser.Order;

class LocalTraversalService
{
    public Iterable<TraversalPosition> performExternalEvaluatorTraversal(
        final Node startNode, final Order order,
        final StopEvaluator stopEvaluator,
        final ReturnableEvaluator returnableEvaluator,
        final RelationshipType[] types, final Direction[] directions )
    {
        switch ( order )
        {
            case BREADTH_FIRST:
                return new BredthFirstTraversal( new Position( 0, 0, null,
                    startNode ), stopEvaluator, returnableEvaluator, types,
                    directions );
            case DEPTH_FIRST:
                return new DepthFirstTraversal( new Position( 0, 0, null,
                    startNode ), stopEvaluator, returnableEvaluator, types,
                    directions );
            default:
                throw new IllegalArgumentException(
                    "Unsupported traversal order: " + order );
        }
    }

    private static class BredthFirstTraversal extends
        Traversal<Queue<Expansion>>
    {
        BredthFirstTraversal( Position position, StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator, RelationshipType[] types,
            Direction[] directions )
        {
            super( position, stopEvaluator, returnableEvaluator, types,
                directions );
        }

        @Override
        Expansion current( Queue<Expansion> store )
        {
            return store.peek();
        }

        @Override
        void extendStore( Queue<Expansion> store, Expansion expand )
        {
            store.add( expand );
        }

        @Override
        Queue<Expansion> initStore()
        {
            return new LinkedList<Expansion>();
        }

        @Override
        void removeCurrent( Queue<Expansion> store )
        {
            store.poll();
        }
    }

    private static class DepthFirstTraversal extends
        Traversal<Stack<Expansion>>
    {
        DepthFirstTraversal( Position position, StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator, RelationshipType[] types,
            Direction[] directions )
        {
            super( position, stopEvaluator, returnableEvaluator, types,
                directions );
        }

        @Override
        Expansion current( Stack<Expansion> store )
        {
            if ( store.isEmpty() )
            {
                return null;
            }
            return store.peek();
        }

        @Override
        void extendStore( Stack<Expansion> store, Expansion expand )
        {
            store.push( expand );
        }

        @Override
        Stack<Expansion> initStore()
        {
            return new Stack<Expansion>();
        }

        @Override
        void removeCurrent( Stack<Expansion> store )
        {
            if ( !store.isEmpty() )
            {
                store.pop();
            }
        }
    }

    private static abstract class Traversal<S> implements
        Iterable<TraversalPosition>
    {
        private final Position start;
        private final StopEvaluator stopEvaluator;
        private final ReturnableEvaluator returnableEvaluator;
        private final RelationshipType[] types;
        private final Direction[] directions;

        Traversal( Position position, StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator, RelationshipType[] types,
            Direction[] directions )
        {
            this.start = position;
            this.stopEvaluator = stopEvaluator;
            this.returnableEvaluator = returnableEvaluator;
            this.types = types;
            this.directions = directions;
        }

        boolean shouldReturn( Position currentPos )
        {
            return returnableEvaluator.isReturnableNode( currentPos );
        }

        boolean shouldExpand( Position currentPos )
        {
            return !stopEvaluator.isStopNode( currentPos );
        }

        public Iterator<TraversalPosition> iterator()
        {
            return new Iterator<TraversalPosition>()
            {
                final Set<Node> visited = new HashSet<Node>();
                int returned = 0;
                Position current;
                Position last = start;
                S store = initStore();
                {
                    visited.add( start.currentNode() );
                    if ( shouldReturn( start ) )
                    {
                        current = start;
                    }
                    if ( shouldExpand( last ) )
                    {
                        extendStore( store, new Expansion( start, types,
                            directions ) );
                    }
                }

                public boolean hasNext()
                {
                    if ( current != null )
                    {
                        return true;
                    }
                    else if ( last == null )
                    {
                        return false;
                    }
                    else
                    {
                        Position next = expand( last );
                        if ( next != null )
                        {
                            current = next;
                            return true;
                        }
                        else
                        {
                            last = null;
                            return false;
                        }
                    }
                }

                public TraversalPosition next()
                {
                    if ( hasNext() )
                    {
                        last = current;
                        current = null;
                        returned++;
                        return last;
                    }
                    else
                    {
                        throw new NoSuchElementException();
                    }
                }

                public void remove()
                {
                    throw new UnsupportedOperationException();
                }

                private Position expand( Position position )
                {
                    Expansion expansion = current( store );
                    while ( expansion != null )
                    {
                        if ( !expansion.hasNext() )
                        {
                            removeCurrent( store );
                            expansion = current( store );
                            continue;
                        }
                        Relationship relation = expansion.next();
                        if ( !visited.add( expansion.otherNode( relation ) ) )
                        {
                            continue;
                        }
                        Position candidate = expansion.position( returned,
                            relation );
                        if ( shouldExpand( candidate ) )
                        {
                            extendStore( store, new Expansion( candidate,
                                types, directions ) );
                        }
                        if ( shouldReturn( candidate ) )
                        {
                            return candidate;
                        }
                    }
                    return null;
                }

            };
        }

        abstract S initStore();

        abstract void extendStore( S store, Expansion expand );

        abstract Expansion current( S store );

        abstract void removeCurrent( S store );
    }

    private static class Expansion
    {
        private final Position from;
        private final Iterator<Iterator<Relationship>> relations;
        private Iterator<Relationship> current = null;

        Expansion( Position from, RelationshipType[] types,
            Direction[] directions )
        {
            this.from = from;
            @SuppressWarnings( "hiding" )
            List<Iterator<Relationship>> relations = new LinkedList<Iterator<Relationship>>();
            for ( int i = 0; i < types.length; i++ )
            {
                Iterator<Relationship> iter = from.currentNode()
                    .getRelationships( types[ i ], directions[ i ] ).iterator();
                if ( iter.hasNext() )
                {
                    relations.add( iter );
                }
            }
            this.relations = relations.iterator();
            if ( this.relations.hasNext() )
            {
                this.current = this.relations.next();
            }
        }

        Node otherNode( Relationship relationship )
        {
            return relationship.getOtherNode( from.currentNode() );
        }

        Position position( int returned, Relationship rel )
        {
            return new Position( from.depth() + 1, returned, rel,
                otherNode( rel ) );
        }

        boolean hasNext()
        {
            if ( current == null )
            {
                return false;
            }
            else if ( current.hasNext() )
            {
                return true;
            }
            else if ( relations.hasNext() )
            {
                current = relations.next();
                return true;
            }
            else
            {
                current = null;
                return false;
            }
        }

        Relationship next()
        {
            if ( current == null )
            {
                throw new NoSuchElementException();
            }
            return current.next();
        }
    }
}
