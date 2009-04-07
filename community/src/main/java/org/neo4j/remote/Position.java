package org.neo4j.remote;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.TraversalPosition;

class Position implements TraversalPosition
{
    private final int depth;
    private final int returned;
    private final Relationship last;
    private final Node current;

    Position( int depth, int returned, Relationship last, Node current )
    {
        this.depth = depth;
        this.returned = returned;
        this.last = last;
        this.current = current;
    }

    public Node currentNode()
    {
        return current;
    }

    public int depth()
    {
        return depth;
    }

    public boolean isStartNode()
    {
        return last == null;
    }

    public boolean notStartNode()
    {
        return last != null;
    }

    public Relationship lastRelationshipTraversed()
    {
        return last;
    }

    public Node previousNode()
    {
        return last.getOtherNode( current );
    }

    public int returnedNodesCount()
    {
        return returned;
    }
}
