/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.management.impl;

import javax.management.NotCompliantMBeanException;

import org.neo4j.io.pagecache.monitoring.PageCacheCounters;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.management.PageCache;

public final class PageCacheBean extends ManagementBeanProvider
{
    public PageCacheBean()
    {
        super( PageCache.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new PageCacheImpl( management );
    }

    private static class PageCacheImpl extends Neo4jMBean implements PageCache
    {
        private final PageCacheCounters pageCacheCounters;

        PageCacheImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            this.pageCacheCounters = management.resolveDependency( PageCacheCounters.class );
        }

        @Override
        public long getFaults()
        {
            return pageCacheCounters.faults();
        }

        @Override
        public long getEvictions()
        {
            return pageCacheCounters.evictions();
        }

        @Override
        public long getPins()
        {
            return pageCacheCounters.pins();
        }

        @Override
        public long getFlushes()
        {
            return pageCacheCounters.flushes();
        }

        @Override
        public long getBytesRead()
        {
            return pageCacheCounters.bytesRead();
        }

        @Override
        public long getBytesWritten()
        {
            return pageCacheCounters.bytesWritten();
        }

        @Override
        public long getFileMappings()
        {
            return pageCacheCounters.filesMapped();
        }

        @Override
        public long getFileUnmappings()
        {
            return pageCacheCounters.filesUnmapped();
        }

        @Override
        public double getHitRatio()
        {
            return pageCacheCounters.hitRatio();
        }

        @Override
        public long getEvictionExceptions()
        {
            return pageCacheCounters.evictionExceptions();
        }

        @Override
        public double getUsageRatio()
        {
            return pageCacheCounters.usageRatio();
        }
    }
}
