/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.test.rule;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.logging.FormattedLogProvider;

public class ConfigurablePageCacheRule extends PageCacheRule
{
    public PageCache getPageCache( FileSystemAbstraction fs, Config config )
    {
        return getPageCache( fs, config(), config );
    }

    public PageCache getPageCache( FileSystemAbstraction fs, PageCacheConfig pageCacheConfig, Config config )
    {
        closeExistingPageCache();
        pageCache = createPageCache( fs, pageCacheConfig, config );
        pageCachePostConstruct( pageCacheConfig );
        return pageCache;
    }

    private PageCache createPageCache( FileSystemAbstraction fs, PageCacheConfig pageCacheConfig, Config config )
    {
        PageCacheTracer tracer = selectConfig( baseConfig.tracer, pageCacheConfig.tracer, PageCacheTracer.NULL );
        PageCursorTracerSupplier cursorTracerSupplier = selectConfig( baseConfig.pageCursorTracerSupplier,
                pageCacheConfig.pageCursorTracerSupplier, DefaultPageCursorTracerSupplier.INSTANCE );
        config.augmentDefaults( GraphDatabaseSettings.pagecache_memory, "8M" );
        FormattedLogProvider logProvider = FormattedLogProvider.toOutputStream( System.err );
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fs, config, tracer, cursorTracerSupplier, logProvider.getLog( PageCache.class ) )
        {
            @Override
            public int calculatePageSize( Config config, PageSwapperFactory swapperFactory )
            {
                if ( pageCacheConfig.pageSize != null )
                {
                    return pageCacheConfig.pageSize;
                }
                return super.calculatePageSize( config, swapperFactory );
            }
        };
        return pageCacheFactory.getOrCreatePageCache();
    }
}
