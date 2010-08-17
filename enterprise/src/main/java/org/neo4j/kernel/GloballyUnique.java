package org.neo4j.kernel;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.traversal.TraversalBranch;

class GloballyUnique extends AbstractUniquenessFilter
{
    private final Set<Long> visited = new HashSet<Long>();
    
    GloballyUnique( PrimitiveTypeFetcher type )
    {
        super( type );
    }

    public boolean check( TraversalBranch branch, boolean remember )
    {
        long id = type.getId( branch );
        return remember ? visited.add( id ) : !visited.contains( id );
    }
}
