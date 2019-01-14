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
package org.neo4j.kernel.impl.traversal;

import java.util.Iterator;
import java.util.LinkedList;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Paths;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalContext;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.PrefetchingIterator;

import static org.neo4j.helpers.collection.Iterators.asResourceIterator;

class TraversalBranchImpl implements TraversalBranch
{
    final TraversalBranch parent;
    private final Relationship howIGotHere;
    private final Node source;
    private ResourceIterator<Relationship> relationships;
    // high bit here [cidd,dddd][dddd,dddd][dddd,dddd][dddd,dddd]
    private int depthAndEvaluationBits;
    private int expandedCount;

    /*
     * For expansion sources for all nodes except the start node
     */
    TraversalBranchImpl( TraversalBranch parent, int depth, Node source, Relationship toHere )
    {
        this.parent = parent;
        this.source = source;
        this.howIGotHere = toHere;
        this.depthAndEvaluationBits = depth;
    }

    /*
     * For the start node branches
     */
    TraversalBranchImpl( TraversalBranch parent, Node source )
    {
        this.parent = parent;
        this.source = source;
        this.howIGotHere = null;
        this.depthAndEvaluationBits = 0;
    }

    protected void setEvaluation( Evaluation evaluation )
    {
        this.depthAndEvaluationBits &= 0x3FFFFFFF; // First clear those evaluation bits
        this.depthAndEvaluationBits |= bitValue( evaluation.includes(), 30 ) | bitValue( evaluation.continues(), 31 );
    }

    private int bitValue( boolean value, int bit )
    {
        return (value ? 1 : 0) << bit;
    }

    protected void expandRelationships( PathExpander expander )
    {
        if ( continues() )
        {
            relationships = expandRelationshipsWithoutChecks( expander );
        }
        else
        {
            resetRelationships();
        }
    }

    protected ResourceIterator expandRelationshipsWithoutChecks( PathExpander expander )
    {
        return asResourceIterator( expander.expand( this, BranchState.NO_STATE ).iterator() );
    }

    protected boolean hasExpandedRelationships()
    {
        return relationships != null;
    }

    protected void evaluate( TraversalContext context )
    {
        setEvaluation( context.evaluate( this, null ) );
    }

    @Override
    public void initialize( final PathExpander expander, TraversalContext metadata )
    {
        evaluate( metadata );
    }

    @Override
    public TraversalBranch next( PathExpander expander, TraversalContext context )
    {
        if ( relationships == null )
        {
            expandRelationships( expander );
        }
        while ( relationships.hasNext() )
        {
            Relationship relationship = relationships.next();
            if ( relationship.equals( howIGotHere ) )
            {
                context.unnecessaryRelationshipTraversed();
                continue;
            }
            expandedCount++;
            Node node = relationship.getOtherNode( source );
            // TODO maybe an unnecessary instantiation. Instead pass in this+node+relationship to uniqueness check
            TraversalBranch next = newNextBranch( node, relationship );
            if ( context.isUnique( next ) )
            {
                context.relationshipTraversed();
                next.initialize( expander, context );
                return next;
            }
            else
            {
                context.unnecessaryRelationshipTraversed();
            }
        }
        resetRelationships();
        return null;
    }

    protected TraversalBranch newNextBranch( Node node, Relationship relationship )
    {
        return new TraversalBranchImpl( this, length() + 1, node, relationship );
    }

    @Override
    public void prune()
    {
        resetRelationships();
    }

    private void resetRelationships()
    {
        if ( relationships != null )
        {
            relationships.close();
        }
        relationships = Iterators.emptyResourceIterator();
    }

    @Override
    public int length()
    {
        return depthAndEvaluationBits & 0x3FFFFFFF;
    }

    @Override
    public TraversalBranch parent()
    {
        return this.parent;
    }

    @Override
    public int expanded()
    {
        return expandedCount;
    }

    @Override
    public boolean includes()
    {
        return (depthAndEvaluationBits & 0x40000000) != 0;
    }

    @Override
    public boolean continues()
    {
        return (depthAndEvaluationBits & 0x80000000) != 0;
    }

    @Override
    public void evaluation( Evaluation eval )
    {
        setEvaluation( Evaluation.of( includes() & eval.includes(), continues() & eval.continues() ) );
    }

    @Override
    public Node startNode()
    {
        return findStartBranch().endNode();
    }

    private TraversalBranch findStartBranch()
    {
        TraversalBranch branch = this;
        while ( branch.length() > 0 )
        {
            branch = branch.parent();
        }
        return branch;
    }

    @Override
    public Node endNode()
    {
        return source;
    }

    @Override
    public Relationship lastRelationship()
    {
        return howIGotHere;
    }

    @Override
    public Iterable<Relationship> relationships()
    {
        LinkedList<Relationship> relationships = new LinkedList<>();
        TraversalBranch branch = this;
        while ( branch.length() > 0 )
        {
            relationships.addFirst( branch.lastRelationship() );
            branch = branch.parent();
        }
        return relationships;
    }

    @Override
    public Iterable<Relationship> reverseRelationships()
    {
        return () -> new PrefetchingIterator<Relationship>()
        {
            private TraversalBranch branch = TraversalBranchImpl.this;

            @Override
            protected Relationship fetchNextOrNull()
            {
                try
                {
                    return branch != null ? branch.lastRelationship() : null;
                }
                finally
                {
                    branch = branch != null ? branch.parent() : null;
                }
            }
        };
    }

    @Override
    public Iterable<Node> nodes()
    {
        LinkedList<Node> nodes = new LinkedList<>();
        TraversalBranch branch = this;
        while ( branch.length() > 0 )
        {
            nodes.addFirst( branch.endNode() );
            branch = branch.parent();
        }
        nodes.addFirst( branch.endNode() );
        return nodes;
    }

    @Override
    public Iterable<Node> reverseNodes()
    {
        return () -> new PrefetchingIterator<Node>()
        {
            private TraversalBranch branch = TraversalBranchImpl.this;

            @Override
            protected Node fetchNextOrNull()
            {
                try
                {
                    return branch.length() >= 0 ? branch.endNode() : null;
                }
                finally
                {
                    branch = branch.parent();
                }
            }
        };
    }

    @Override
    public Iterator<PropertyContainer> iterator()
    {
        LinkedList<PropertyContainer> entities = new LinkedList<>();
        TraversalBranch branch = this;
        while ( branch.length() > 0 )
        {
            entities.addFirst( branch.endNode() );
            entities.addFirst( branch.lastRelationship() );
            branch = branch.parent();
        }
        entities.addFirst( branch.endNode() );
        return entities.iterator();
    }

    @Override
    public int hashCode()
    {
        TraversalBranch branch = this;
        int hashCode = 1;
        while ( branch.length() > 0 )
        {
            Relationship relationship = branch.lastRelationship();
            hashCode = 31 * hashCode + relationship.hashCode();
            branch = branch.parent();
        }
        if ( hashCode == 1 )
        {
            hashCode = endNode().hashCode();
        }
        return hashCode;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj == this )
        {
            return true;
        }
        if ( !( obj instanceof TraversalBranch ) )
        {
            return false;
        }

        TraversalBranch branch = this;
        TraversalBranch other = (TraversalBranch) obj;
        if ( branch.length() != other.length() )
        {
            return false;
        }

        while ( branch.length() > 0 )
        {
            if ( !branch.lastRelationship().equals( other.lastRelationship() ) )
            {
                return false;
            }
            branch = branch.parent();
            other = other.parent();
        }
        return true;
    }

    @Override
    public String toString()
    {
        return Paths.defaultPathToString( this );
    }

    @Override
    public Object state()
    {
        return null;
    }
}
