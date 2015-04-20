/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com.storecopy;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.InternalAbstractGraphDatabase;

/**
 * A PageCache implementation that delegates to another page cache, whose life cycle is managed elsewhere.
 *
 * This page cache implementation DOES NOT delegate close() method calls, so it can be used to safely share a page
 * cache with a component that might try to close the page cache it gets.
 */
public class ExternallyManagedPageCache implements PageCache
{
    private final PageCache delegate;

    public ExternallyManagedPageCache( PageCache delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public void close() throws IOException
    {
        // Don't close the delegate, because we are not in charge of its life cycle.
    }

    @Override
    public PagedFile map( File file, int pageSize ) throws IOException
    {
        return delegate.map( file, pageSize );
    }

    @Override
    public void flushAndForce() throws IOException
    {
        delegate.flushAndForce();
    }

    @Override
    public int pageSize()
    {
        return delegate.pageSize();
    }

    @Override
    public int maxCachedPages()
    {
        return delegate.maxCachedPages();
    }

    /**
     * Create a GraphDatabaseFactory that will build EmbeddedGraphDatabase instances that all use the given page cache.
     */
    public static GraphDatabaseFactory graphDatabaseFactoryWithPageCache( final PageCache delegatePageCache )
    {
        return new GraphDatabaseFactory()
        {
            @Override
            protected GraphDatabaseService newDatabase( String path, Map<String,String> config,
                                                        InternalAbstractGraphDatabase.Dependencies dependencies )
            {
                return new EmbeddedGraphDatabase( path, config, dependencies )
                {
                    @Override
                    protected PageCache createPageCache()
                    {
                        return new ExternallyManagedPageCache( delegatePageCache );
                    }
                };
            }
        };
    }
}
