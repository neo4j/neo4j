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
package org.neo4j.tooling.wrap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser;
import org.neo4j.helpers.collection.IteratorWrapper;

class WrappedTraverser extends WrappedObject<Traverser> implements Traverser
{
    WrappedTraverser( WrappedGraphDatabase graphdb, Traverser traverser )
    {
        super( graphdb, traverser );
    }

    @Override
    public TraversalPosition currentPosition()
    {
        return new Position( graphdb, wrapped.currentPosition() );
    }

    @Override
    public Collection<Node> getAllNodes()
    {
        Collection<Node> all = wrapped.getAllNodes(), wrapped = new ArrayList<Node>( all.size() );
        for ( Node node : all )
        {
            wrapped.add( graphdb.node( node, false ) );
        }
        return wrapped;
    }

    @Override
    public Iterator<Node> iterator()
    {
        return new IteratorWrapper<Node, Node>( wrapped.iterator() )
        {
            @Override
            protected Node underlyingObjectToObject( Node object )
            {
                return graphdb.node( object, false );
            }
        };
    }

    static class Evaluator implements StopEvaluator, ReturnableEvaluator
    {
        private final WrappedGraphDatabase graphdb;
        private final StopEvaluator stopEvaluator;
        private final ReturnableEvaluator returnableEvaluator;

        Evaluator( WrappedGraphDatabase graphdb, StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator )
        {
            this.graphdb = graphdb;
            this.stopEvaluator = stopEvaluator;
            this.returnableEvaluator = returnableEvaluator;
        }

        @Override
        public boolean isStopNode( TraversalPosition currentPos )
        {
            return stopEvaluator.isStopNode( new Position( graphdb, currentPos ) );
        }

        @Override
        public boolean isReturnableNode( TraversalPosition currentPos )
        {
            return returnableEvaluator.isReturnableNode( new Position( graphdb, currentPos ) );
        }
    }

    private static class Position extends WrappedObject<TraversalPosition> implements TraversalPosition
    {
        Position( WrappedGraphDatabase graphdb, TraversalPosition pos )
        {
            super( graphdb, pos );
        }

        @Override
        public Node currentNode()
        {
            return graphdb.node( wrapped.currentNode(), false );
        }

        @Override
        public Node previousNode()
        {
            return graphdb.node( wrapped.previousNode(), false );
        }

        @Override
        public Relationship lastRelationshipTraversed()
        {
            return graphdb.relationship( wrapped.lastRelationshipTraversed(), false );
        }

        @Override
        public int depth()
        {
            return wrapped.depth();
        }

        @Override
        public int returnedNodesCount()
        {
            return wrapped.returnedNodesCount();
        }

        @Override
        public boolean notStartNode()
        {
            return wrapped.notStartNode();
        }

        @Override
        public boolean isStartNode()
        {
            return wrapped.isStartNode();
        }
    }
}
