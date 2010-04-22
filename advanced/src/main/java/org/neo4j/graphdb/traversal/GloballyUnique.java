package org.neo4j.graphdb.traversal;

import java.util.HashSet;
import java.util.Set;

class GloballyUnique extends UniquenessFilter
{
    private final Set<Long> visited = new HashSet<Long>();

    GloballyUnique( PrimitiveTypeFetcher type )
    {
        super( type );
    }

    @Override
    boolean check( long id, boolean remember )
    {
        if ( remember )
        {
            return visited.add( id );
        }
        else
        {
            return !visited.contains( id );
        }
    }
}
