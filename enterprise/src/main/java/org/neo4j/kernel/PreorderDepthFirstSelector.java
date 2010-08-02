package org.neo4j.kernel;

import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.BranchSelector;

/**
 * Selects {@link TraversalBranch}s according to preorder depth first pattern,
 * the most natural ordering in a depth first search, see
 * http://en.wikipedia.org/wiki/Depth-first_search
 */
class PreorderDepthFirstSelector implements BranchSelector
{
    private TraversalBranch current;
    
    PreorderDepthFirstSelector( TraversalBranch startSource )
    {
        this.current = startSource;
    }
    
    public TraversalBranch next()
    {
        TraversalBranch result = null;
        while ( result == null )
        {
            if ( current == null )
            {
                return null;
            }
            TraversalBranch next = current.next();
            if ( next == null )
            {
                current = current.parent();
                continue;
            }
            else
            {
                current = next;
            }
            if ( current != null )
            {
                result = current;
            }
        }
        return result;
    }
}
