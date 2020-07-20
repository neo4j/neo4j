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
package org.neo4j.kernel.impl.storemigration;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseConfig;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;

public class DatabaseMigratorFactory
{
    private final FileSystemAbstraction fs;
    private final Config config;
    private final LogService logService;
    private final PageCache pageCache;
    private final JobScheduler jobScheduler;
    private final NamedDatabaseId namedDatabaseId;
    private final PageCacheTracer pageCacheTracer;
    private final MemoryTracker memoryTracker;
    private final DatabaseHealth databaseHealth;

    public DatabaseMigratorFactory( FileSystemAbstraction fs, Config config, LogService logService, PageCache pageCache, JobScheduler jobScheduler,
            NamedDatabaseId namedDatabaseId, PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker, DatabaseHealth databaseHealth )
    {
        this.fs = fs;
        this.config = config;
        this.logService = logService;
        this.pageCache = pageCache;
        this.jobScheduler = jobScheduler;
        this.namedDatabaseId = namedDatabaseId;
        this.pageCacheTracer = pageCacheTracer;
        this.memoryTracker = memoryTracker;
        this.databaseHealth = databaseHealth;
    }

    public DatabaseMigrator createDatabaseMigrator( DatabaseLayout databaseLayout, StorageEngineFactory storageEngineFactory,
            DependencyResolver dependencies )
    {
        DatabaseConfig dbConfig = new DatabaseConfig( config, namedDatabaseId );
        return new DatabaseMigrator( fs, dbConfig, logService, dependencies, pageCache, jobScheduler, databaseLayout,
                storageEngineFactory, pageCacheTracer, memoryTracker, databaseHealth );
    }
}
