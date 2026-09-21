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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.internal.schema.SchemaCommand;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexProvidersAccess;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;

/**
 * The index-population side of applying {@link SchemaCommand}s in a record-format batch import.
 * Lifecycle: {@link #create} writes the schema rules and opens population resources;
 * {@link #nodeMonitors()}/{@link #relationshipMonitors()} supply the {@link SchemaMonitors} to pass to the
 * node/relationship import stages (which validate and write the populated indexes at the end of each stage);
 * {@link #completeAfterImport()} backfills empty indexes for commands whose schema received no data and
 * checkpoints index statistics; {@link #close} releases resources and must always run, also on failure.
 * <p>
 * The monitors are owned by the caller, not by the population - see {@link #nodeMonitors()}.
 */
public interface ImporterSchemaPopulation extends Closeable {
    /** No-op population used when the import carries no schema commands. */
    ImporterSchemaPopulation NONE = new ImporterSchemaPopulation() {
        @Override
        public SchemaMonitors nodeMonitors() {
            return SchemaMonitors.NO_SCHEMA;
        }

        @Override
        public SchemaMonitors relationshipMonitors() {
            return SchemaMonitors.NO_SCHEMA;
        }

        @Override
        public void completeAfterImport() {}

        @Override
        public void close() {}
    };

    static ImporterSchemaPopulation create(
            List<SchemaCommand> commands,
            BatchingNeoStores neoStores,
            FileSystemAbstraction fileSystem,
            Supplier<IndexProvidersAccess> indexProvidersAccess,
            DatabaseLayout databaseLayout,
            Configuration configuration,
            Config dbConfig,
            JobScheduler jobScheduler,
            PageCacheTracer pageCacheTracer,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker)
            throws IOException {
        if (commands.isEmpty()) {
            return NONE;
        }
        return DefaultImporterSchemaPopulation.create(
                commands,
                neoStores,
                fileSystem,
                indexProvidersAccess,
                databaseLayout,
                configuration,
                dbConfig,
                jobScheduler,
                pageCacheTracer,
                contextFactory,
                memoryTracker);
    }

    /**
     * The returned monitors are owned by the caller, which must close them once the node import stage has
     * finished and before {@link #completeAfterImport()}. Closing is what validates the populators and writes
     * the populated indexes to the target - a caller that skips it silently ends up with unpopulated indexes.
     * {@link #NONE} hands back the shared no-op {@link SchemaMonitors#NO_SCHEMA}; a real population builds a
     * fresh instance per call, so the two stages never share one.
     */
    SchemaMonitors nodeMonitors();

    /** As {@link #nodeMonitors()}, for the relationship import stage. */
    SchemaMonitors relationshipMonitors();

    void completeAfterImport() throws IOException;
}
