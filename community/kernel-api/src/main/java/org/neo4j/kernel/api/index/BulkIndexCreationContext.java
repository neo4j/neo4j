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
package org.neo4j.kernel.api.index;

import org.neo4j.batchimport.api.IndexImporterFactory.CreationContext;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.MetadataCache;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.ReadableStorageEngine;
import org.neo4j.token.TokenHolders;

/**
 * Context container for the dependencies required to perform the bulk creation of indexes
 */
public record BulkIndexCreationContext(
        Config config,
        ReadableStorageEngine storageEngine,
        DatabaseLayout databaseLayout,
        FileSystemAbstraction fileSystem,
        PageCache pageCache,
        MetadataCache metadataCache,
        JobScheduler jobScheduler,
        TokenHolders tokenHolders,
        CursorContextFactory contextFactory,
        PageCacheTracer pageCacheTracer,
        LogService logService,
        MemoryTracker memoryTracker)
        implements CreationContext {}
