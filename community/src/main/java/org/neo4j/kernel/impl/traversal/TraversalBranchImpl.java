package org.neo4j.kernel.impl.traversal;

import java.util.Collections;
import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.kernel.impl.traversal.TraverserImpl.TraverserIterator;

class TraversalBranchImpl implements TraversalBranch
{
    private final TraversalBranch parent;
    private final Node source;
    private Iterator<Relationship> relationships;
    private final Relationship howIGotHere;
    private final int depth;
    final TraverserIterator traverser;
    private Path path;
    private int expandedCount;

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
        expandRelationships( true );
    }

    /*
     * For the start node expansion source
     */
    TraversalBranchImpl( TraverserIterator  traverser, Node source,
            RelationshipExpander expander )
    {
        this.traverser = traverser;
        this.parent = null;
        this.source = source;
        this.howIGotHere = null;
        this.depth = 0;
    }

    protected void expandRelationships( boolean doChecks )
    {
        boolean okToExpand = !doChecks || traverser.shouldExpandBeyond( this );
        relationships = okToExpand ?
                traverser.description.expander.expand( source ).iterator() :
                Collections.<Relationship>emptyList().iterator();
    }

    protected boolean hasExpandedRelationships()
    {
        return relationships != null;
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
                return next;
            }
        }
        return null;
    }

    public Path position()
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
}
