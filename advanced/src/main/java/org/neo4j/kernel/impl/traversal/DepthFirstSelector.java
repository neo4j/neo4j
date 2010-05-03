package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.SourceSelector;
import org.neo4j.graphdb.traversal.TraversalRules;

class DepthFirstSelector implements SourceSelector
{
    private ExpansionSource current;
    
    DepthFirstSelector( ExpansionSource startSource )
    {
        this.current = startSource;
    }
    
    public ExpansionSource nextPosition( TraversalRules rules )
    {
        ExpansionSource result = null;
        while ( result == null )
        {
            if ( current == null )
            {
                return null;
            }
            ExpansionSource next = current.next( rules );
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
                if ( rules.okToReturn( current ) )
                {
                    result = current;
                }
            }
        }
        return result;
    }
}
