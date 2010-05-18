package org.neo4j.kernel;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.SourceSelector;

/**
 * Selects {@link ExpansionSource}s according to postorder breadth first
 * pattern which basically is a reverse to preorder breadth first in that
 * deepest levels are returned first, see
 * http://en.wikipedia.org/wiki/Breadth-first_search
 */
class PostorderBreadthFirstSelector implements SourceSelector
{
    private Iterator<ExpansionSource> sourceIterator;
    private ExpansionSource current;
    
    PostorderBreadthFirstSelector( ExpansionSource startSource )
    {
        this.current = startSource;
    }

    public ExpansionSource nextPosition()
    {
        if ( sourceIterator == null )
        {
            sourceIterator = gatherSourceIterator();
        }
        return sourceIterator.hasNext() ? sourceIterator.next() : null;
    }

    private Iterator<ExpansionSource> gatherSourceIterator()
    {
        LinkedList<ExpansionSource> queue = new LinkedList<ExpansionSource>();
        queue.add( current.next() );
        while ( true )
        {
            List<ExpansionSource> level = gatherOneLevel( queue );
            if ( level.isEmpty() )
            {
                break;
            }
            queue.addAll( 0, level );
        }
        return queue.iterator();
    }

    private List<ExpansionSource> gatherOneLevel(
            List<ExpansionSource> queue )
    {
        List<ExpansionSource> level = new LinkedList<ExpansionSource>();
        Integer depth = null;
        for ( ExpansionSource source : queue )
        {
            if ( depth == null )
            {
                depth = source.depth();
            }
            else if ( source.depth() != depth )
            {
                break;
            }
            
            while ( true )
            {
                ExpansionSource next = source.next();
                if ( next == null )
                {
                    break;
                }
                level.add( next );
            }
        }
        return level;
    }
}
