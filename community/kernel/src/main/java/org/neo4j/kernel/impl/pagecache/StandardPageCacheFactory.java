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

import org.neo4j.helpers.Service;
import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.RunnablePageCache;
import org.neo4j.io.pagecache.impl.standard.StandardPageCache;

@Service.Implementation( PageCacheFactory.class )
public class StandardPageCacheFactory extends PageCacheFactory
{
    public StandardPageCacheFactory()
    {
        super( "standard" );
    }

    @Override
    public RunnablePageCache createPageCache(
            PageSwapperFactory swapperFactory,
            int maxPages,
            int cachePageSize,
            PageCacheMonitor monitor )
    {
        return new StandardPageCache( swapperFactory, maxPages, cachePageSize, monitor );
    }

    @Override
    public int getPriority()
    {
        return 1;
    }

    @Override
    public String getImplementationName()
    {
        return StandardPageCache.class.getSimpleName();
    }
}
