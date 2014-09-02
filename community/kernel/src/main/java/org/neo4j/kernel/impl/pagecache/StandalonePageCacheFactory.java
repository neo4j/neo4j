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
package org.neo4j.kernel.impl.pagecache;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;

/*
 * This class is an helper to allow to construct properly a page cache in the few places we need it without all
 * the graph database stuff, e.g., some tests, store dumps (see, DumpStore) and for some bits when upgrading.
 * That said, this is not to be used to create a page cache for the regular use case since that one should be
 * properly integrated with monitors and the scheduler into neo4j.
 */
public class StandalonePageCacheFactory
{
    public static PageCache createPageCache( FileSystemAbstraction fileSystem, String schedulerName,
            LifeSupport life )
    {
        final int pageSize = 8192;
        final int maxPages = 1000;
        Config config = new Config( MapUtil.stringMap(
                GraphDatabaseSettings.mapped_memory_page_size.name(), "" + pageSize,
                GraphDatabaseSettings.mapped_memory_total_size.name(), "" + (pageSize * maxPages) )
        );
        Neo4jJobScheduler scheduler = life.add( new Neo4jJobScheduler( schedulerName ) );
        LifecycledPageCache pageCache = life.add( new LifecycledPageCache(
                new StandardPageCacheFactory(),
                new SingleFilePageSwapperFactory( fileSystem ),
                scheduler,
                config,
                PageCacheMonitor.NULL ) );
        return pageCache;
    }
}
