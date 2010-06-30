package org.neo4j.graphalgo.impl.util;

import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.SourceSelector;

/**
 * A preorder depth first selector which detects "super nodes", i.e. nodes
 * which has many relationships. It delays traversing those super nodes until
 * after all non-super nodes have been traversed.
 * 
 * @author Mattias Persson
 * @author Tobias Ivarsson
 */
public class LiteDepthFirstSelector implements SourceSelector
{
    private final Queue<ExpansionSource> superNodes = new LinkedList<ExpansionSource>();
    private ExpansionSource current;
    private final int threshold;
    
    public LiteDepthFirstSelector( ExpansionSource startSource, int startThreshold )
    {
        this.current = startSource;
        this.threshold = startThreshold;
    }
    
    public ExpansionSource nextPosition()
    {
        ExpansionSource result = null;
        while ( result == null )
        {
            if ( current == null )
            {
                current = superNodes.poll();
                if ( current == null )
                {
                    return null;
                }
            }
            else if ( current.expanded() > 0 && current.expanded() % threshold == 0 )
            {
                superNodes.add( current );
                current = current.parent();
                continue;
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
