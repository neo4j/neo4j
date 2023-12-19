/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
