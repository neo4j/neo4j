package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.TraversalBranch;

public final class FinalExpansionSource implements TraversalBranch
{
    private final Node head;
    private final Relationship[] path;

    public FinalExpansionSource( Node head, Relationship... path )
    {
        this.head = head;
        this.path = path;
    }

    public int depth()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * Returns <code>null</code> since {@link FinalExpansionSource} does not
     * expand.
     */
    public TraversalBranch next()
    {
        return null;
    }

    public Node node()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public TraversalBranch parent()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Path position()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Relationship relationship()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public int expanded()
    {
        return 0;
    }
}
