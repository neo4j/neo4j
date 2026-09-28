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
import org.neo4j.kernel.KernelVersionProviders;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.wal.LogFormatVersionProvider;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.LogTailMetadata;
import org.neo4j.wal.WALService;
import org.neo4j.wal.entry.LogFormat;
import org.neo4j.wal.files.LogFilesBuilder;

public class LogTailExtractor {
    private final FileSystemAbstraction fs;
    private final Config config;
    private final StorageEngineFactory storageEngineFactory;
    private final DatabaseTracers databaseTracers;
    private final boolean readOnly;
    private final LogPosition maxPosition;

    public LogTailExtractor(
            FileSystemAbstraction fs,
            Config config,
            StorageEngineFactory storageEngineFactory,
            DatabaseTracers databaseTracers) {
        this(fs, config, storageEngineFactory, databaseTracers, true, LogPosition.UNSPECIFIED);
    }

    public LogTailExtractor(
            FileSystemAbstraction fs,
            Config config,
            StorageEngineFactory storageEngineFactory,
            DatabaseTracers databaseTracers,
            boolean readOnly,
            LogPosition maxPosition) {
        this.fs = fs;
        this.config = config;
        this.storageEngineFactory = storageEngineFactory;
        this.databaseTracers = databaseTracers;
        this.readOnly = readOnly;
        this.maxPosition = maxPosition;
    }

    /**
     * Only use this version if you are sure you do not have empty tx logs or are sure that kernel version will not be asked for.
     */
    public LogTailMetadata getTailMetadata(DatabaseLayout databaseLayout, MemoryTracker memoryTracker)
            throws IOException {
        return buildLogFiles(databaseLayout, memoryTracker, KernelVersionProviders.latestFromConfig(config))
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

    private WALService buildLogFiles(
            DatabaseLayout databaseLayout, MemoryTracker memoryTracker, KernelVersionProvider kernelVersionProvider)
            throws IOException {
        LogFormatVersionProvider emptyLogsFormatProvider =
                () -> LogFormat.fromConfigAndKernelVersion(config, kernelVersionProvider.kernelVersion());
        var builder = readOnly
                ? LogFilesBuilder.readableBuilder(databaseLayout, fs, kernelVersionProvider, emptyLogsFormatProvider)
                // Writeable to allow CheckpointFile to cleanup files without header on recovery
                : LogFilesBuilder.writeableBuilder(databaseLayout, fs, kernelVersionProvider, emptyLogsFormatProvider)
                        .withNoPreallocation();
        return builder.withConfig(config)
                .withMemoryTracker(memoryTracker)
                .withDatabaseTracers(databaseTracers)
                .withStorageEngineFactory(storageEngineFactory)
                .withNoInit()
                .withTailReadingMaxPosition(maxPosition)
                .build();
    }
}
