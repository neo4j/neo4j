package org.neo4j.kernel;

import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.SourceSelector;

/**
 * Selects {@link ExpansionSource}s according to postorder depth first pattern,
 * see http://en.wikipedia.org/wiki/Depth-first_search
 */
class PostorderDepthFirstSelector implements SourceSelector
{
    private ExpansionSource current;
    
    PostorderDepthFirstSelector( ExpansionSource startSource )
    {
        this.current = startSource;
    }
    
    public ExpansionSource nextPosition()
    {
        ExpansionSource result = null;
        while ( result == null )
        {
            if ( current == null )
            {
                return null;
            }
            
            ExpansionSource next = current.next();
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
