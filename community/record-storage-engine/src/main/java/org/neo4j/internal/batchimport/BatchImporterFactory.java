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

import java.util.NoSuchElementException;
import java.util.function.Supplier;
import org.neo4j.annotations.service.Service;
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
import org.neo4j.kernel.DatabaseCreationOptions;
import org.neo4j.kernel.api.index.IndexProvidersAccess;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.service.NamedService;
import org.neo4j.service.Services;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.wal.LogTailMetadata;

@Service
public abstract class BatchImporterFactory implements NamedService {
    /**
     * For importers that will never apply schema commands. Fails with a diagnosable error instead of a bare
     * {@link NullPointerException} if an {@link org.neo4j.batchimport.api.input.Input} with schema commands
     * is imported anyway.
     */
    public static final Supplier<IndexProvidersAccess> NO_INDEX_PROVIDERS = () -> {
        throw new IllegalStateException(
                "This batch importer was created without index providers and cannot apply schema commands");
    };

    private final int priority;

    protected BatchImporterFactory(int priority) {
        this.priority = priority;
    }

    public abstract BatchImporter instantiate(
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
            JobScheduler jobScheduler,
            Collector badCollector,
            LogFilesInitializer logFilesInitializer,
            IndexImporterFactory indexImporterFactory,
            MemoryTracker memoryTracker,
            CursorContextFactory contextFactory,
            Supplier<IndexProvidersAccess> indexProvidersAccess,
            DatabaseCreationOptions databaseCreationOptions);

    public static BatchImporterFactory withHighestPriority() {
        return HighestPriorityFactoryHolder.FACTORY;
    }

    private static final class HighestPriorityFactoryHolder {
        private static final BatchImporterFactory FACTORY = loadFactory();

        private static BatchImporterFactory loadFactory() {
            BatchImporterFactory highestPrioritized = null;
            for (BatchImporterFactory candidate : Services.loadAll(BatchImporterFactory.class)) {
                if (highestPrioritized == null || candidate.priority > highestPrioritized.priority) {
                    highestPrioritized = candidate;
                }
            }
            if (highestPrioritized == null) {
                throw new NoSuchElementException("No batch importers found");
            }
            return highestPrioritized;
        }
    }
}
