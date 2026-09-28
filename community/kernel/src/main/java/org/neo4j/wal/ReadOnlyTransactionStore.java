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
package org.neo4j.wal;

import java.io.IOException;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.wal.files.LogFilesBuilder;

/**
 * Used for reading transactions off of file.
 */
public class ReadOnlyTransactionStore implements LogicalTransactionStore {
    private final LogicalTransactionStore physicalStore;

    public ReadOnlyTransactionStore(
            FileSystemAbstraction fs,
            DatabaseLayout fromDatabaseLayout,
            Config config,
            Monitors monitors,
            CommandReaderFactory commandReaderFactory)
            throws IOException {
        this(fs, fromDatabaseLayout, config, monitors, commandReaderFactory, false);
    }

    public ReadOnlyTransactionStore(
            FileSystemAbstraction fs,
            DatabaseLayout fromDatabaseLayout,
            Config config,
            Monitors monitors,
            CommandReaderFactory commandReaderFactory,
            boolean failOnUnsupportedLogVersion)
            throws IOException {
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache();
        LogFilesBuilder logFilesBuilder = LogFilesBuilder.readableBuilder(
                        fromDatabaseLayout,
                        fs,
                        KernelVersionProvider.THROWING_PROVIDER,
                        LogFormatVersionProvider.THROWING_PROVIDER)
                .withCommandReaderFactory(commandReaderFactory)
                .withConfig(config)
                .withInitializeProviders();
        if (failOnUnsupportedLogVersion) {
            logFilesBuilder = logFilesBuilder.withFailOnUnsupportedLogVersion();
        }
        WALService walService = logFilesBuilder.build();
        physicalStore = new PhysicalLogicalTransactionStore(
                walService,
                transactionMetadataCache,
                commandReaderFactory,
                monitors,
                true,
                config,
                EmptyMemoryTracker.INSTANCE);
    }

    @Override
    public CommandBatchCursor getCommandBatches(long appendIndexToStartFrom) throws IOException {
        return physicalStore.getCommandBatches(appendIndexToStartFrom);
    }

    @Override
    public CommandBatchCursor getCommandBatches(LogPosition position) throws IOException {
        return physicalStore.getCommandBatches(position);
    }

    @Override
    public CommandBatchCursor getCommandBatches(
            LogPosition position, LogPosition maxPosition, boolean treatBrokenLastEntryAsCorruption)
            throws IOException {
        return physicalStore.getCommandBatches(position, maxPosition, treatBrokenLastEntryAsCorruption);
    }

    @Override
    public CommandBatchCursor getCommandBatchesInReverseOrder(LogPosition backToPosition, LogPosition maxPosition)
            throws IOException {
        return physicalStore.getCommandBatchesInReverseOrder(backToPosition, maxPosition);
    }
}
