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

import java.util.Comparator;

import org.neo4j.helpers.Service;
import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.RunnablePageCache;

public abstract class PageCacheFactory extends Service
{
    public static final Comparator<PageCacheFactory> orderByHighestPriorityFirst = new Comparator<PageCacheFactory>()
    {
        @Override
        public int compare( PageCacheFactory o1, PageCacheFactory o2 )
        {
            int p1 = o1.getPriority();
            int p2 = o2.getPriority();
            return p1 - p2;
        }
    };

    /**
     * Create a new instance of a service implementation identified with the
     * specified key(s).
     *
     * @param key     the main key for identifying this service implementation
     */
    protected PageCacheFactory( String key )
    {
        super( key );
    }

    public abstract RunnablePageCache createPageCache(
            PageSwapperFactory swapperFactory,
            int maxPages,
            int cachePageSize,
            PageCacheMonitor monitor );

    public abstract int getPriority();

    public abstract String getImplementationName();
}
