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
package org.neo4j.io.enterprise.pagecache.impl.muninn;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.RunnablePageCache;
import org.neo4j.io.pagecache.stress.SimplePageCacheFactory;

public class SimpleMuninnPageCacheFactory implements SimplePageCacheFactory
{
    @Override
    public RunnablePageCache createPageCache( int numberOfCachePages, int cachePageSize, PageCacheMonitor monitor )
    {
        return new MuninnPageCache( new DefaultFileSystemAbstraction(), numberOfCachePages, cachePageSize, monitor );
    }

    @Override
    public String toString()
    {
        return "Muninn page cache";
    }
}
