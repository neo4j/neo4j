/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.time.ZoneId;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.FormattedLogProvider;

/*
 * This class is an helper to allow to construct properly a page cache in the few places we need it without all
 * the graph database stuff, e.g., various store dump programs.
 *
 * All other places where a "proper" page cache is available, e.g. in store migration, should have that one injected.
 * And tests should use the ConfigurablePageCacheRule.
 */
public final class ConfigurableStandalonePageCacheFactory
{
    private ConfigurableStandalonePageCacheFactory()
    {
    }

    public static PageCache createPageCache( FileSystemAbstraction fileSystem )
    {
        return createPageCache( fileSystem, PageCacheTracer.NULL, DefaultPageCursorTracerSupplier.INSTANCE,
                Config.defaults(), EmptyVersionContextSupplier.EMPTY );
    }

    public static PageCache createPageCache( FileSystemAbstraction fileSystem, Config config )
    {
        return createPageCache( fileSystem, PageCacheTracer.NULL, DefaultPageCursorTracerSupplier.INSTANCE, config,
                EmptyVersionContextSupplier.EMPTY );
    }

    /**
     * Create page cache
     * @param fileSystem file system that page cache will be based on
     * @param pageCacheTracer global page cache tracer
     * @param pageCursorTracerSupplier supplier of thread local (transaction local) page cursor tracer that will provide
     * thread local page cache statistics
     * @param config page cache configuration
     * @param versionContextSupplier version context supplier
     * @return created page cache instance
     */
    public static PageCache createPageCache( FileSystemAbstraction fileSystem, PageCacheTracer pageCacheTracer,
            PageCursorTracerSupplier pageCursorTracerSupplier, Config config,
            VersionContextSupplier versionContextSupplier )
    {
        config.augmentDefaults( GraphDatabaseSettings.pagecache_memory, "8M" );
        ZoneId logTimeZone = config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
        FormattedLogProvider logProvider = FormattedLogProvider.withZoneId( logTimeZone ).toOutputStream( System.err );
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fileSystem, config, pageCacheTracer, pageCursorTracerSupplier,
                logProvider.getLog( PageCache.class ), versionContextSupplier );
        return pageCacheFactory.getOrCreatePageCache();
    }
}
