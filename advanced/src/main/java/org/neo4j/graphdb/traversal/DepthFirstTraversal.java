package org.neo4j.graphdb.traversal;

import org.neo4j.graphdb.Node;

class DepthFirstTraversal extends Traversal
{
    private ExpansionSource current;

    DepthFirstTraversal( TraversalDescription description, Node startNode )
    {
        super( description );
        this.current = new ExpansionSource( this, null, startNode,
                description.expander, null );
    }

    @Override
    protected Position fetchNextOrNull()
    {
        Position position = null;
        while ( position == null )
        {
            if ( current == null )
            {
                return null;
            }
            ExpansionSource next = current.next( this );
            if ( next == null )
            {
                current = current.parent;
                continue;
            }
            else
            {
                current = next;
            }
            if ( current != null )
            {
                position = current.position( this );
            }
        }
        return position;
    }
}
