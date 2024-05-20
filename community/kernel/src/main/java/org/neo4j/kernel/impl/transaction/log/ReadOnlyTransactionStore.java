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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.CommandReaderFactory;

/**
 * Used for reading transactions off of file.
 */
public class ReadOnlyTransactionStore implements Lifecycle, LogicalTransactionStore {
    private final LifeSupport life = new LifeSupport();
    private final LogicalTransactionStore physicalStore;

    public ReadOnlyTransactionStore(
            FileSystemAbstraction fs,
            DatabaseLayout fromDatabaseLayout,
            Config config,
            Monitors monitors,
            CommandReaderFactory commandReaderFactory)
            throws IOException {
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache();
        LogFiles logFiles = LogFilesBuilder.activeFilesBuilder(
                        fromDatabaseLayout, fs, KernelVersionProvider.THROWING_PROVIDER)
                .withCommandReaderFactory(commandReaderFactory)
                .withConfig(config)
                .build();
        physicalStore = new PhysicalLogicalTransactionStore(
                logFiles, transactionMetadataCache, commandReaderFactory, monitors, true, config, fs);
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
    public CommandBatchCursor getCommandBatchesInReverseOrder(LogPosition backToPosition) throws IOException {
        return physicalStore.getCommandBatchesInReverseOrder(backToPosition);
    }

    @Override
    public void init() {
        life.init();
    }

    @Override
    public void start() {
        life.start();
    }

    @Override
    public void stop() {
        life.stop();
    }

    @Override
    public void shutdown() {
        life.shutdown();
    }
}
