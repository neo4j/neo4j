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
package org.neo4j.internal.batchimport;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.batchimport.api.AdditionalInitialIds;
import org.neo4j.batchimport.api.BatchImporter;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.IndexImporterFactory;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.LogFilesInitializer;

@ServiceProvider
public class StandardBatchImporterFactory extends BatchImporterFactory {
    public StandardBatchImporterFactory() {
        super(1);
    }

    @Override
    public String getName() {
        return "standard";
    }

    @Override
    public BatchImporter instantiate(
            DatabaseLayout directoryStructure,
            FileSystemAbstraction fileSystem,
            PageCacheTracer cacheTracer,
            Configuration config,
            LogService logService,
            ExecutionMonitor executionMonitor,
            AdditionalInitialIds additionalInitialIds,
            LogTailMetadata logTailMetadata,
            Config dbConfig,
            Monitor monitor,
            JobScheduler scheduler,
            Collector badCollector,
            LogFilesInitializer logFilesInitializer,
            IndexImporterFactory indexImporterFactory,
            MemoryTracker memoryTracker,
            CursorContextFactory contextFactory) {
        return new ParallelBatchImporter(
                directoryStructure,
                fileSystem,
                cacheTracer,
                config,
                logService,
                executionMonitor,
                additionalInitialIds,
                logTailMetadata,
                dbConfig,
                monitor,
                scheduler,
                badCollector,
                logFilesInitializer,
                indexImporterFactory,
                memoryTracker,
                contextFactory);
    }
}
