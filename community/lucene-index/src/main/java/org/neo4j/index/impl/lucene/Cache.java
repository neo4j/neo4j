/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.impl.lucene;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.cache.LruCache;

public class Cache
{
    private final Map<IndexIdentifier, Map<String,LruCache<String,Collection<Long>>>> caching = 
            Collections.synchronizedMap( 
                    new HashMap<IndexIdentifier, Map<String,LruCache<String,Collection<Long>>>>() );
    
    public void setCapacity( IndexIdentifier identifier, String key, int size )
    {
        Map<String, LruCache<String, Collection<Long>>> map = caching.get( identifier );
        if ( map == null )
        {
            map = new HashMap<String, LruCache<String,Collection<Long>>>();
            caching.put( identifier, map );
        }
        map.put( key, new LruCache<String, Collection<Long>>( key, size ) );
    }
    
    public LruCache<String, Collection<Long>> get( IndexIdentifier identifier, String key )
    {
        Map<String, LruCache<String, Collection<Long>>> map = caching.get( identifier );
        return map != null ? map.get( key ) : null;
    }
    
    public void disable( IndexIdentifier identifier, String key )
    {
        Map<String, LruCache<String, Collection<Long>>> map = caching.get( identifier );
        if ( map != null )
        {
            map.remove( key );
        }
    }
    
    public void disable( IndexIdentifier identifier )
    {
        Map<String, LruCache<String, Collection<Long>>> map = caching.get( identifier );
        if ( map != null )
        {
            map.clear();
        }
    }
}
