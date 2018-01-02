/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.FormattedLogProvider;

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

    public static PageCache createPageCache( FileSystemAbstraction fileSystem )
    {
        return createPageCache( fileSystem, new Config() );
    }

    public static PageCache createPageCache(
            FileSystemAbstraction fileSystem, Config config )
    {
        Config baseConfig = new Config( MapUtil.stringMap(
                GraphDatabaseSettings.pagecache_memory.name(), "8M" ) );
        Config finalConfig = baseConfig.with( config.getParams() );
        FormattedLogProvider logProvider = FormattedLogProvider.toOutputStream( System.err );
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fileSystem, finalConfig, PageCacheTracer.NULL, logProvider.getLog( PageCache.class ) );
        return pageCacheFactory.getOrCreatePageCache();
    }
}
