package org.neo4j.graphdb.traversal;

import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.graphdb.Node;

public class BreadthFirstTraversal extends Traversal
{
    private final Queue<ExpansionSource> queue = new LinkedList<ExpansionSource>();
    private ExpansionSource current = null;

    BreadthFirstTraversal( TraversalDescription description, Node startNode )
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
            ExpansionSource next = current.next( this );
            if ( next != null )
            {
                queue.add( next );
                position = next.position( this );
            }
            else
            {
                current = queue.poll();
                if ( current == null )
                {
                    return null;
                }
            }
        }
        return position;
    }
}
