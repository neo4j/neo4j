package org.neo4j.kernel.impl.traversal;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.graphdb.Path;
import org.neo4j.helpers.Predicate;

class MultiFilter implements Predicate<Path>
{
    private final Collection<Predicate<Path>> filters = new ArrayList<Predicate<Path>>();
    
    MultiFilter( Predicate<Path>... filters )
    {
        for ( Predicate<Path> filter : filters )
        {
            this.filters.add( filter );
        }
    }

    MultiFilter( Collection<Predicate<Path>> filters )
    {
        this.filters.addAll( filters );
    }

    public boolean accept( Path path )
    {
        for ( Predicate<Path> filter : this.filters )
        {
            if ( !filter.accept( path ) )
            {
                return false;
            }
        }
        return true;
    }

    public MultiFilter add( Predicate<Path> filter )
    {
        Collection<Predicate<Path>> newFilters = new ArrayList<Predicate<Path>>( this.filters );
        newFilters.add( filter );
        return new MultiFilter( newFilters );
    }
}
