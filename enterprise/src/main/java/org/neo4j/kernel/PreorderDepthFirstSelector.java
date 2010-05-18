package org.neo4j.kernel;

import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.SourceSelector;

/**
 * Selects {@link ExpansionSource}s according to preorder depth first pattern,
 * the most natural ordering in a depth first search, see
 * http://en.wikipedia.org/wiki/Depth-first_search
 */
class PreorderDepthFirstSelector implements SourceSelector
{
    private ExpansionSource current;
    
    PreorderDepthFirstSelector( ExpansionSource startSource )
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
