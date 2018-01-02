/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Given a PageCache instance, this LifecycleAdapter will close it upon shutdown.
 *
 * This way, the PageCache can participate in our life cycle mechanism without knowing it.
 */
public class PageCacheLifecycle extends LifecycleAdapter
{
    private final PageCache pageCache;

    public PageCacheLifecycle( PageCache pageCache )
    {
        this.pageCache = pageCache;
    }

    @Override
    public void shutdown() throws Throwable
    {
        pageCache.close();
    }
}
