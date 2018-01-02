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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.BranchSelector;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalContext;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.UniquenessFactory;

/**
 * A {@link TraversalBranch} that abstracts the fact that it is actually
 * potentially several branches, i.e. several start nodes. A
 * {@link TraversalDescription#traverse(Node...)} call can supply more than
 * one starting {@link Node} and for implementation simplicity a
 * {@link BranchSelector} starts from one {@link TraversalBranch}.
 * This class bridges that gap.
 * 
 * @author Mattias Persson
 */
class AsOneStartBranch implements TraversalBranch
{
    private Iterator<TraversalBranch> branches;
    private int expanded;
    private final TraversalContext context;
    private final InitialBranchState initialState;
    private final UniquenessFactory uniqueness;

    AsOneStartBranch( TraversalContext context, Iterable<Node> nodes, InitialBranchState initialState, UniquenessFactory uniqueness )
    {
        this.context = context;
        this.initialState = initialState;
        this.uniqueness = uniqueness;
        this.branches = toBranches( nodes );
    }

    private Iterator<TraversalBranch> toBranches( Iterable<Node> nodes )
    {
        if ( uniqueness.eagerStartBranches() )
        {
            List<TraversalBranch> result = new ArrayList<TraversalBranch>();
            for ( Node node : nodes )
                result.add( new StartNodeTraversalBranch( context, this, node, initialState ) );
            return result.iterator();
        } else
        {
            return new TraversalBranchIterator( nodes.iterator() );
        }
    }

    @Override
    public TraversalBranch parent()
    {
        return null;
    }

    @Override
    public int length()
    {
        return -1;
    }

    @Override
    public Node endNode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Relationship lastRelationship()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TraversalBranch next( PathExpander expander, TraversalContext metadata )
    {
        if ( branches.hasNext() )
        {
            expanded++;
            return branches.next().next( expander, metadata );
        }
        return null;
    }

    @Override
    public int expanded()
    {
        return expanded;
    }

    @Override
    public boolean continues()
    {
        return true;
    }
    
    @Override
    public boolean includes()
    {
        return false;
    }
    
    @Override
    public void evaluation( Evaluation eval )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void initialize( PathExpander expander, TraversalContext metadata )
    {
    }

    @Override
    public Node startNode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Relationship> relationships()
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Iterable<Relationship> reverseRelationships()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Node> nodes()
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Iterable<Node> reverseNodes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<PropertyContainer> iterator()
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void prune()
    {
        branches = Collections.<TraversalBranch>emptyList().iterator();
    }
    
    @Override
    public Object state()
    {
        throw new UnsupportedOperationException();
    }

    private class TraversalBranchIterator implements Iterator<TraversalBranch>
    {
        private final Iterator<Node> nodeIterator;

        public TraversalBranchIterator( Iterator<Node> nodeIterator )
        {
            this.nodeIterator = nodeIterator;
        }

        @Override
        public boolean hasNext()
        {
            return nodeIterator.hasNext();
        }

        @Override
        public TraversalBranch next()
        {
            return new StartNodeTraversalBranch( context, AsOneStartBranch.this, nodeIterator.next(),
                    initialState);
        }

        @Override
        public void remove()
        {
            nodeIterator.remove();
        }
    }
}
