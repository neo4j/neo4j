package org.neo4j.kernel;

import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.BranchSelector;

/**
 * Selects {@link TraversalBranch}s according to postorder depth first pattern,
 * see http://en.wikipedia.org/wiki/Depth-first_search
 */
class PostorderDepthFirstSelector implements BranchSelector
{
    private TraversalBranch current;
    
    PostorderDepthFirstSelector( TraversalBranch startSource )
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
            if ( next != null )
            {
                current = next;
            }
            else
            {
                result = current;
                current = current.parent();
            }
        }
        return result;
    }
}
