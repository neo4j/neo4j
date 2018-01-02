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

import org.neo4j.helpers.Service;
import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.management.PageCache;

@Service.Implementation(ManagementBeanProvider.class)
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
        private final PageCacheMonitor pageCacheMonitor;

        PageCacheImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            this.pageCacheMonitor = management.resolveDependency( PageCacheMonitor.class );
        }

        @Override
        public long getFaults()
        {
            return pageCacheMonitor.countFaults();
        }

        @Override
        public long getEvictions()
        {
            return pageCacheMonitor.countEvictions();
        }

        @Override
        public long getPins()
        {
            return pageCacheMonitor.countPins();
        }

        @Override
        public long getUnpins()
        {
            return pageCacheMonitor.countUnpins();
        }

        @Override
        public long getFlushes()
        {
            return pageCacheMonitor.countFlushes();
        }

        @Override
        public long getBytesRead()
        {
            return pageCacheMonitor.countBytesRead();
        }

        @Override
        public long getBytesWritten()
        {
            return pageCacheMonitor.countBytesWritten();
        }

        @Override
        public long getFileMappings()
        {
            return pageCacheMonitor.countFilesMapped();
        }

        @Override
        public long getFileUnmappings()
        {
            return pageCacheMonitor.countFilesUnmapped();
        }

        @Override
        public long getEvictionExceptions()
        {
            return pageCacheMonitor.countEvictionExceptions();
        }
    }
}
