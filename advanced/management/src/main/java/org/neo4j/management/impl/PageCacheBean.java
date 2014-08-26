/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.io.pagecache.CountingPageCacheMonitor;
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
        private final CountingPageCacheMonitor pageCacheMonitor;

        PageCacheImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            this.pageCacheMonitor = management.resolveDependency( CountingPageCacheMonitor.class );
        }

        @Override
        public int getFaults()
        {
            return pageCacheMonitor.countFaults();
        }

        @Override
        public int getEvictions()
        {
            return pageCacheMonitor.countEvictions();
        }

        @Override
        public int getPins()
        {
            return pageCacheMonitor.countPins();
        }

        @Override
        public int getUnpins()
        {
            return pageCacheMonitor.countUnpins();
        }

        @Override
        public int getTakenExclusiveLocks()
        {
            return pageCacheMonitor.countTakenExclusiveLocks();
        }

        @Override
        public int getTakenSharedLocks()
        {
            return pageCacheMonitor.countTakenSharedLocks();
        }

        @Override
        public int getReleasedExclusiveLocks()
        {
            return pageCacheMonitor.countReleasedExclusiveLocks();
        }

        @Override
        public int getReleasedSharedLocks()
        {
            return pageCacheMonitor.countReleasedSharedLocks();
        }

        @Override
        public int getFlushes()
        {
            return pageCacheMonitor.countFlushes();
        }


    }
}
