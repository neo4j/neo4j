/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.pagecache;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;

/*
 * This class is an helper to allow to construct properly a page cache in the few places we need it without all
 * the graph database stuff, e.g., various store dump programs.
 *
 * All other places where a "proper" page cache is available, e.g. in store migration, should have that one injected.
 * And tests should use the PageCacheRule.
 */
public final class StandalonePageCacheFactory
{
    private StandalonePageCacheFactory()
    {
        // Not constructable.
    }

    public static StandalonePageCache createPageCache( FileSystemAbstraction fileSystem, String pageCacheName )
    {
        return createPageCache( fileSystem, new Config(), pageCacheName );
    }

    public static StandalonePageCache createPageCache(
            FileSystemAbstraction fileSystem, Config config, String pageCacheName )
    {
        final LifeSupport life = new LifeSupport();
        final String qualifiedPageCacheName = "StandalonePageCache[" + pageCacheName + "]";

        Neo4jJobScheduler scheduler = life.add( new Neo4jJobScheduler( qualifiedPageCacheName ) );
        SingleFilePageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory( fileSystem );

        Config baseConfig = new Config( MapUtil.stringMap(
                GraphDatabaseSettings.pagecache_memory.name(), "8M" ) );
        Config finalConfig = baseConfig.with( config.getParams() );
        LifecycledPageCache delegate = life.add(
                new LifecycledPageCache( swapperFactory, scheduler, finalConfig, PageCacheTracer.NULL ) );
        life.start();

        return new DelegatingStandalonePageCache( delegate, life, qualifiedPageCacheName );
    }

    private static final class DelegatingStandalonePageCache implements StandalonePageCache
    {
        private final LifecycledPageCache delegate;
        private final LifeSupport life;
        private final String pageCacheName;

        private DelegatingStandalonePageCache(
                LifecycledPageCache delegate,
                LifeSupport life,
                String pageCacheName )
        {
            this.delegate = delegate;
            this.life = life;
            this.pageCacheName = pageCacheName;
        }

        @Override
        public PagedFile map( File file, int pageSize ) throws IOException
        {
            return delegate.map( file, pageSize );
        }

        @Override
        public void flush() throws IOException
        {
            delegate.flush();
        }

        @Override
        public void close()
        {
            life.shutdown();
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

        @Override
        public String toString()
        {
            return pageCacheName;
        }
    }
}
