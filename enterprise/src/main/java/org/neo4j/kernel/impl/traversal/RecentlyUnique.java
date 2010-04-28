package org.neo4j.kernel.impl.traversal;

import org.neo4j.kernel.impl.cache.LruCache;

class RecentlyUnique extends UniquenessFilter
{
    private static final Object PLACE_HOLDER = new Object();
    private static final int DEFAULT_RECENT_SIZE = 10000; 
    
    private final LruCache<Long, Object> recentlyVisited;
    
    RecentlyUnique( PrimitiveTypeFetcher type, Object parameter )
    {
        super( type );
        parameter = parameter != null ? parameter : DEFAULT_RECENT_SIZE;
        recentlyVisited = new LruCache<Long, Object>( "Recently visited",
                ((Number) parameter).intValue(), null );
    }

    @Override
    boolean check( long id, boolean remember )
    {
        boolean add = recentlyVisited.get( id ) == null;
        if ( add&remember )
        {
            recentlyVisited.put( id, PLACE_HOLDER );
        }
        return add;
    }
}
