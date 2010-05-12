package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.Position;

public final class FinalExpansionSource implements ExpansionSource
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
    public ExpansionSource next()
    {
        return null;
    }

    public Node node()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public ExpansionSource parent()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Position position()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Relationship relationship()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
