package org.neo4j.server.rest.web.paging;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Traverser;

public class PagedTraverser implements Iterator<List<Path>>, Iterable<List<Path>>, Leasable
{

    private final int pageSize;
    private Iterator<Path> iterator;

    public PagedTraverser( Traverser traverser, int pageSize )
    {
        iterator = traverser.iterator();
        this.pageSize = pageSize;
    }

    public List<Path> next()
    {
        if(!iterator.hasNext()) {
            return null;
        }
        
        ArrayList<Path> result = new ArrayList<Path>();

        for ( int i = 0; i < pageSize; i++ )
        {
            if(!iterator.hasNext()) {
                break;
            } else {
                result.add( iterator.next() );
            }
        }

        return result;
    }

    @Override
    public boolean hasNext()
    {
        return iterator.hasNext();
    }


    @Override
    public void remove()
    {
        iterator.remove(); 
    }

    @Override
    public Iterator<List<Path>> iterator()
    {
        return this;
    }

}
