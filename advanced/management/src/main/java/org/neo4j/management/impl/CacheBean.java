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
package org.neo4j.management.impl;

import java.util.Collection;
import java.util.LinkedList;
import javax.management.NotCompliantMBeanException;
import org.neo4j.helpers.Service;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.core.Caches;

@Service.Implementation( ManagementBeanProvider.class )
public class CacheBean extends ManagementBeanProvider
{
    public CacheBean()
    {
        super( org.neo4j.management.Cache.class );
    }

    @Override
    protected Iterable<? extends Neo4jMBean> createMBeans( ManagementData management )
            throws NotCompliantMBeanException
    {
        Caches caches = management.getKernelData().graphDatabase().getDependencyResolver()
                .resolveDependency( Caches.class );
        Collection<CacheManager> cacheBeans = new LinkedList<>();
        cacheBeans.add( new CacheManager( management, caches.getProvider().getDescription(), caches.node() ) );
        cacheBeans.add( new CacheManager( management, caches.getProvider().getDescription(), caches.relationship() ) );
        return cacheBeans;
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        throw new UnsupportedOperationException( "Uses createMBeans" );
    }

    private class CacheManager extends Neo4jMBean implements org.neo4j.management.Cache
    {
        private final Cache cache;
        private final String description;

        CacheManager( ManagementData management, String description, Cache cache )
                throws NotCompliantMBeanException
        {
            super( management, cache.getName() );
            this.cache = cache;
            this.description = description;
        }

        public String getCacheType()
        {
            return description;
        }

        public void clear()
        {
            cache.clear();
        }

        @Override
        public long getCacheSize()
        {
            return cache.size();
        }

        @Override
        public long getHitCount()
        {
            return cache.hitCount();
        }

        @Override
        public long getMissCount()
        {
            return cache.missCount();
        }
    }
}
