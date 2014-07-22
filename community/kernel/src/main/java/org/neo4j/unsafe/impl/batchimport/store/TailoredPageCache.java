/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.store;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;

/**
 * A {@link PageCache} that can assign specific {@link PageCache page caches} tailored
 * for each individual store.
 */
public class TailoredPageCache implements PageCache
{
    private final PageCache defaultPageCache;
    private final Map<String,PageCache> overrides = new HashMap<>();
    private final Map<File,PageCache> mappedPageFiles = new HashMap<>();

    public TailoredPageCache( PageCache defaultPageCache )
    {
        this.defaultPageCache = defaultPageCache;
    }

    public void override( String storeNamePart, PageCache override )
    {
        overrides.put( NeoStore.DEFAULT_NAME + storeNamePart, override );
    }

    @Override
    public PagedFile map( File file, int pageSize ) throws IOException
    {
        PageCache override = overrides.get( file.getName() );
        PageCache factory = override != null ? override : defaultPageCache;
        mappedPageFiles.put( file, factory );
        return factory.map( file, pageSize );
    }

    private Iterable<PageCache> allPageCaches()
    {
        Set<PageCache> pageCaches = new HashSet<>( overrides.values() );
        pageCaches.add( defaultPageCache );
        return pageCaches;
    }

    @Override
    public void unmap( File file ) throws IOException
    {
        PageCache pageCache = mappedPageFiles.remove( file );
        if ( pageCache == null )
        {
            throw new IllegalArgumentException( file + " not mapped" );
        }
        pageCache.unmap( file );
    }

    @Override
    public void flush() throws IOException
    {
        for ( PageCache pageCache : allPageCaches() )
        {
            pageCache.flush();
        }
    }

    @Override
    public void close() throws IOException
    {
        for ( PageCache pageCache : allPageCaches() )
        {
            pageCache.close();
        }
    }

    @Override
    public int pageSize()
    {
        return defaultPageCache.pageSize();
    }

    @Override
    public int maxCachedPages()
    {
        return defaultPageCache.maxCachedPages();
    }
}
