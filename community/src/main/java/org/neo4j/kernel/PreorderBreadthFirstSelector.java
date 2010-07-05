package org.neo4j.kernel;

import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.SourceSelector;

/**
 * Selects {@link ExpansionSource}s according to breadth first
 * pattern, the most natural ordering in a breadth first search, see
 * http://en.wikipedia.org/wiki/Breadth-first_search
 */
class PreorderBreadthFirstSelector implements SourceSelector
{
    private final Queue<ExpansionSource> queue = new LinkedList<ExpansionSource>();
    private ExpansionSource current;
    
    PreorderBreadthFirstSelector( ExpansionSource startSource )
    {
        this.current = startSource;
    }

    public ExpansionSource nextPosition()
    {
        ExpansionSource result = null;
        while ( result == null )
        {
            ExpansionSource next = current.next();
            if ( next != null )
            {
                queue.add( next );
                result = next;
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
        return result;
    }
}
