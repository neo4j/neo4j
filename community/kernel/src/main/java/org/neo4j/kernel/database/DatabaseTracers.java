/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.database;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.version.VersionStorageTracer;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.lock.LockTracer;

public class DatabaseTracers {
    public static final DatabaseTracers EMPTY =
            new DatabaseTracers(DatabaseTracer.NULL, LockTracer.NONE, PageCacheTracer.NULL, VersionStorageTracer.NULL);

    private final DatabaseTracer databaseTracer;
    private final LockTracer lockTracer;
    private final PageCacheTracer pageCacheTracer;
    private final VersionStorageTracer versionStorageTracer;

    public DatabaseTracers(Tracers tracers, NamedDatabaseId namedDatabaseId) {
        this(
                tracers.getDatabaseTracer(namedDatabaseId),
                tracers.getLockTracer(),
                tracers.getPageCacheTracer(),
                tracers.getVersionStorageTracer(namedDatabaseId));
    }

    public DatabaseTracers(
            DatabaseTracer databaseTracer,
            LockTracer lockTracer,
            PageCacheTracer pageCacheTracer,
            VersionStorageTracer versionStorageTracer) {
        this.databaseTracer = databaseTracer;
        this.lockTracer = lockTracer;
        this.pageCacheTracer = pageCacheTracer;
        this.versionStorageTracer = versionStorageTracer;
    }

    public DatabaseTracer getDatabaseTracer() {
        return databaseTracer;
    }

    public LockTracer getLockTracer() {
        return lockTracer;
    }

    public PageCacheTracer getPageCacheTracer() {
        return pageCacheTracer;
    }

    public VersionStorageTracer getVersionStorageTracer() {
        return versionStorageTracer;
    }
}
