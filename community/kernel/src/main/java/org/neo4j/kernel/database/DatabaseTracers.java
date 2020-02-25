/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.database;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.lock.LockTracer;

public class DatabaseTracers
{
    public static final DatabaseTracers EMPTY = new DatabaseTracers( DatabaseTracer.NULL, LockTracer.NONE, PageCacheTracer.NULL );

    private final DatabaseTracer databaseTracer;
    private final LockTracer lockTracer;
    private final PageCacheTracer pageCacheTracer;

    public DatabaseTracers( Tracers tracers )
    {
        this( tracers.getDatabaseTracer(), tracers.getLockTracer(), tracers.getPageCacheTracer() );
    }

    public DatabaseTracers( DatabaseTracer databaseTracer, LockTracer lockTracer, PageCacheTracer pageCacheTracer )
    {
        this.databaseTracer = databaseTracer;
        this.lockTracer = lockTracer;
        this.pageCacheTracer = pageCacheTracer;
    }

    public DatabaseTracer getDatabaseTracer()
    {
        return databaseTracer;
    }

    public LockTracer getLockTracer()
    {
        return lockTracer;
    }

    public PageCacheTracer getPageCacheTracer()
    {
        return pageCacheTracer;
    }
}
