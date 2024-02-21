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
package org.neo4j.kernel.monitoring.tracing;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.version.VersionStorageTracer;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.lock.LockTracer;

public interface Tracers {

    Tracers EMPTY_TRACERS = new Tracers() {
        @Override
        public PageCacheTracer getPageCacheTracer() {
            return PageCacheTracer.NULL;
        }

        @Override
        public LockTracer getLockTracer() {
            return LockTracer.NONE;
        }

        @Override
        public DatabaseTracer getDatabaseTracer(NamedDatabaseId namedDatabaseId) {
            return DatabaseTracer.NULL;
        }

        @Override
        public VersionStorageTracer getVersionStorageTracer(NamedDatabaseId namedDatabaseId) {
            return VersionStorageTracer.NULL;
        }
    };

    PageCacheTracer getPageCacheTracer();

    LockTracer getLockTracer();

    DatabaseTracer getDatabaseTracer(NamedDatabaseId namedDatabaseId);

    VersionStorageTracer getVersionStorageTracer(NamedDatabaseId namedDatabaseId);
}
