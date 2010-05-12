package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.SourceSelector;

class DepthFirstSelector implements SourceSelector
{
    private ExpansionSource current;
    
    DepthFirstSelector( ExpansionSource startSource )
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
