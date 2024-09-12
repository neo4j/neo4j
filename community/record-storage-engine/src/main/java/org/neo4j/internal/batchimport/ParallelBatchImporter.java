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

import java.io.IOException;
import org.neo4j.batchimport.api.AdditionalInitialIds;
import org.neo4j.batchimport.api.BatchImporter;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.IndexImporterFactory;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.MetadataCache;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.LogFilesInitializer;

/**
 * {@link BatchImporter} which tries to exercise as much of the available resources to gain performance.
 * Or rather ensure that the slowest resource (usually I/O) is fully saturated and that enough work is
 * being performed to keep that slowest resource saturated all the time.
 * <p>
 * Overall goals: split up processing cost by parallelizing. Keep CPUs busy, keep I/O busy and writing sequentially.
 * I/O is only allowed to be read to and written from sequentially, any random access drastically reduces performance.
 * Goes through multiple stages where each stage has one or more steps executing in parallel, passing
 * batches between these steps through each stage, i.e. passing batches downstream.
 */
public class ParallelBatchImporter implements BatchImporter {
    private static final String BATCH_IMPORTER_CHECKPOINT = "Batch importer checkpoint.";
    private final RecordDatabaseLayout databaseLayout;
    private final FileSystemAbstraction fileSystem;
    private final PageCacheTracer pageCacheTracer;
    private final Configuration config;
    private final LogService logService;
    private final LogTailMetadata logTailMetadata;
    private final Config dbConfig;
    private final ExecutionMonitor executionMonitor;
    private final AdditionalInitialIds additionalInitialIds;
    private final Monitor monitor;
    private final JobScheduler jobScheduler;
    private final Collector badCollector;
    private final LogFilesInitializer logFilesInitializer;
    private final IndexImporterFactory indexImporterFactory;
    private final MemoryTracker memoryTracker;
    private final CursorContextFactory contextFactory;

    public ParallelBatchImporter(
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fileSystem,
            PageCacheTracer pageCacheTracer,
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
            CursorContextFactory contextFactory) {
        this.databaseLayout = RecordDatabaseLayout.convert(databaseLayout);
        this.fileSystem = fileSystem;
        this.pageCacheTracer = pageCacheTracer;
        this.config = config;
        this.logService = logService;
        this.logTailMetadata = logTailMetadata;
        this.dbConfig = dbConfig;
        this.executionMonitor = executionMonitor;
        this.additionalInitialIds = additionalInitialIds;
        this.monitor = monitor;
        this.jobScheduler = jobScheduler;
        this.badCollector = badCollector;
        this.logFilesInitializer = logFilesInitializer;
        this.indexImporterFactory = indexImporterFactory;
        this.memoryTracker = memoryTracker;
        this.contextFactory = contextFactory;
    }

    @Override
    public void doImport(Input input) throws IOException {
        if (!input.schemaCommands().isEmpty()) {
            throw new UnsupportedOperationException("Record format batch import does not support schema changes");
        }

        try (BatchingNeoStores store = ImportLogic.instantiateNeoStores(
                        fileSystem,
                        databaseLayout,
                        pageCacheTracer,
                        config,
                        logService,
                        additionalInitialIds,
                        logTailMetadata,
                        dbConfig,
                        jobScheduler,
                        memoryTracker,
                        contextFactory);
                ImportLogic logic = new ImportLogic(
                        databaseLayout,
                        store,
                        config,
                        dbConfig,
                        logService,
                        executionMonitor,
                        badCollector,
                        monitor,
                        contextFactory,
                        indexImporterFactory,
                        pageCacheTracer,
                        memoryTracker)) {
            store.createNew();
            logic.initialize(input);
            logic.importNodes();
            logic.prepareIdMapper();
            logic.importRelationships();
            logic.calculateNodeDegrees();
            logic.linkRelationshipsOfAllTypes();
            logic.defragmentRelationshipGroups();
            logFilesInitializer.initializeLogFiles(
                    databaseLayout,
                    store.getNeoStores().getMetaDataStore(),
                    new MetadataCache(logTailMetadata),
                    fileSystem,
                    BATCH_IMPORTER_CHECKPOINT);
            logic.buildAuxiliaryStores();
            logic.success();
        }
    }
}
