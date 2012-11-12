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

import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.kernel.impl.traversal.TraverserImpl.TraverserIterator;

class TraversalBranchImpl implements TraversalBranch//, Path
{
    private static final Iterator<Relationship> EMPTY_ITERATOR = new Iterator<Relationship>()
    {
        @Override
        public boolean hasNext()
        {
            return false;
        }

        @Override
        public Relationship next()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    };
    
    private final TraversalBranch parent;
    private final Node source;
    private Iterator<Relationship> relationships;
    private final Relationship howIGotHere;
    private final int depth;
    final TraverserIterator traverser;
    private Path path;
    private int expandedCount;
    private Evaluation evaluation;

    /*
     * For expansion sources for all nodes except the start node
     */
    TraversalBranchImpl( TraverserIterator traverser, TraversalBranch parent, int depth,
            Node source, RelationshipExpander expander, Relationship toHere )
    {
        this.traverser = traverser;
        this.parent = parent;
        this.source = source;
        this.howIGotHere = toHere;
        this.depth = depth;
    }

    @Override
    public String toString()
    {
        return "TraversalBranch[source=" + source + ",howIGotHere=" + howIGotHere + ",depth=" + depth + "]";
    }

    /*
     * For the start node expansion source
     */
    TraversalBranchImpl( TraverserIterator traverser, Node source,
            RelationshipExpander expander )
    {
        this.traverser = traverser;
        this.parent = null;
        this.source = source;
        this.howIGotHere = null;
        this.depth = 0;
        this.evaluation = traverser.description.evaluator.evaluate( position() );
    }

    protected void expandRelationships()
    {
        if ( evaluation.continues() )
        {
            expandRelationshipsWithoutChecks();
        }
        else
        {
            relationships = EMPTY_ITERATOR;
        }
    }
    
    protected void expandRelationshipsWithoutChecks()
    {
        relationships = traverser.description.expander.expand( source ).iterator();
    }

    protected boolean hasExpandedRelationships()
    {
        return relationships != null;
    }

    public void initialize()
    {
        evaluation = traverser.description.evaluator.evaluate( position() );
        expandRelationships();
    }

    public TraversalBranch next()
    {
        while ( relationships.hasNext() )
        {
            Relationship relationship = relationships.next();
            if ( relationship.equals( howIGotHere ) )
            {
                continue;
            }
            expandedCount++;
            Node node = relationship.getOtherNode( source );
            TraversalBranch next = new TraversalBranchImpl( traverser, this, depth + 1, node,
                    traverser.description.expander, relationship );
            if ( traverser.okToProceed( next ) )
            {
                next.initialize();
                return next;
            }
        }
        return null;
    }

    public Path position()
    {
        return ensurePathInstantiated();
    }

    private Path ensurePathInstantiated()
    {
        if ( this.path == null )
        {
            this.path = new TraversalPath( this );
        }
        return this.path;
    }

    public int depth()
    {
        return depth;
    }

    public Relationship relationship()
    {
        return howIGotHere;
    }

    public Node node()
    {
        return source;
    }

    public TraversalBranch parent()
    {
        return this.parent;
    }

    public int expanded()
    {
        return expandedCount;
    }

    public Evaluation evaluation()
    {
        return evaluation;
    }

//    public Node startNode()
//    {
//        return ensurePathInstantiated().startNode();
//    }
//
//    public Node endNode()
//    {
//        return source;
//    }
//
//    public Relationship lastRelationship()
//    {
//        return howIGotHere;
//    }
//
//    public Iterable<Relationship> relationships()
//    {
//        return ensurePathInstantiated().relationships();
//    }
//
//    public Iterable<Node> nodes()
//    {
//        return ensurePathInstantiated().nodes();
//    }
//
//    public int length()
//    {
//        return depth;
//    }
//
//    public Iterator<PropertyContainer> iterator()
//    {
//        return ensurePathInstantiated().iterator();
//    }
}
