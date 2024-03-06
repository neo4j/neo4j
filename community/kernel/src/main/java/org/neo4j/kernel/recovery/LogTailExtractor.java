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
package org.neo4j.kernel.recovery;

import java.io.IOException;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageEngineFactory;

public class LogTailExtractor {
    private final FileSystemAbstraction fs;
    private final Config config;
    private final StorageEngineFactory storageEngineFactory;
    private final DatabaseTracers databaseTracers;
    private final boolean readOnly;

    public LogTailExtractor(
            FileSystemAbstraction fs,
            Config config,
            StorageEngineFactory storageEngineFactory,
            DatabaseTracers databaseTracers) {
        this(fs, config, storageEngineFactory, databaseTracers, true);
    }

    public LogTailExtractor(
            FileSystemAbstraction fs,
            Config config,
            StorageEngineFactory storageEngineFactory,
            DatabaseTracers databaseTracers,
            boolean readOnly) {
        this.fs = fs;
        this.config = config;
        this.storageEngineFactory = storageEngineFactory;
        this.databaseTracers = databaseTracers;
        this.readOnly = readOnly;
    }

    /**
     * Only use this version if you are sure you do not have empty tx logs or are sure that kernel version will not be asked for.
     */
    public LogTailMetadata getTailMetadata(DatabaseLayout databaseLayout, MemoryTracker memoryTracker)
            throws IOException {
        return buildLogFiles(databaseLayout, memoryTracker, KernelVersionProvider.THROWING_PROVIDER)
                .getTailMetadata();
    }

    public LogTailMetadata getTailMetadata(
            DatabaseLayout databaseLayout,
            MemoryTracker memoryTracker,
            KernelVersionProvider emptyLogsFallbackKernelVersionProvider)
            throws IOException {
        return buildLogFiles(databaseLayout, memoryTracker, emptyLogsFallbackKernelVersionProvider)
                .getTailMetadata();
    }

    private LogFiles buildLogFiles(
            DatabaseLayout databaseLayout, MemoryTracker memoryTracker, KernelVersionProvider kernelVersionProvider)
            throws IOException {
        var builder = readOnly
                ? LogFilesBuilder.readOnlyBuilder(databaseLayout, fs, kernelVersionProvider)
                : LogFilesBuilder.activeFilesBuilder(databaseLayout, fs, kernelVersionProvider);
        return builder.withConfig(config)
                .withMemoryTracker(memoryTracker)
                .withDatabaseTracers(databaseTracers)
                .withStorageEngineFactory(storageEngineFactory)
                .build();
    }
}
