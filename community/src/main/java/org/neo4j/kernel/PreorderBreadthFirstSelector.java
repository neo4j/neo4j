package org.neo4j.kernel;

import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.BranchSelector;

/**
 * Selects {@link TraversalBranch}s according to breadth first
 * pattern, the most natural ordering in a breadth first search, see
 * http://en.wikipedia.org/wiki/Breadth-first_search
 */
class PreorderBreadthFirstSelector implements BranchSelector
{
    private final Queue<TraversalBranch> queue = new LinkedList<TraversalBranch>();
    private TraversalBranch current;
    
    PreorderBreadthFirstSelector( TraversalBranch startSource )
    {
        this.current = startSource;
    }

    public TraversalBranch next()
    {
        TraversalBranch result = null;
        while ( result == null )
        {
            TraversalBranch next = current.next();
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
