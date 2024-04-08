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
package org.neo4j.kernel.impl.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointInfoFactory.ofLogEntry;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.entry.v50.LogEntryDetachedCheckpointV5_0;
import org.neo4j.kernel.recovery.LogTailExtractor;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
class CheckpointInfoFactoryTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void checkpointInfoOfDetachedCheckpoint42Entry() throws IOException, URISyntaxException {
        var logPosition = new LogPosition(0, 48820);
        var storeId = new StoreId(1645458406002L, 3689108786886031620L, "legacy", "legacy", 1, 1);
        LogPosition position = new LogPosition(0, 448);
        LogPosition positionAfterCheckpoint = new LogPosition(0, 640);
        LogPosition postReaderPosition = new LogPosition(0, 640);
        var restoredTransactionId =
                new TransactionId(73, KernelVersion.V4_4, 614900954, 1645458411645L, UNKNOWN_CONSENSUS_INDEX);

        prepareTestResources();

        LogTailMetadata tailMetadata = new LogTailExtractor(
                        fs, Config.defaults(), StorageEngineFactory.defaultStorageEngine(), DatabaseTracers.EMPTY)
                .getTailMetadata(databaseLayout, EmptyMemoryTracker.INSTANCE);

        var checkpointInfo = tailMetadata.getLastCheckPoint().get();

        assertEquals(logPosition, checkpointInfo.transactionLogPosition());
        assertEquals(storeId, checkpointInfo.storeId());
        assertEquals(position, checkpointInfo.checkpointEntryPosition());
        assertEquals(positionAfterCheckpoint, checkpointInfo.channelPositionAfterCheckpoint());
        assertEquals(postReaderPosition, checkpointInfo.checkpointFilePostReadPosition());
        assertEquals(restoredTransactionId, checkpointInfo.transactionId());
    }

    @Test
    void checkpointInfoOfDetachedCheckpoint50Entry() {
        var logPosition = new LogPosition(0, 1);
        var storeId = new StoreId(4, 5, "engine-1", "format-1", 1, 2);
        LogPosition position = new LogPosition(1, 2);
        LogPosition positionAfterCheckpoint = new LogPosition(3, 4);
        LogPosition postReaderPosition = new LogPosition(5, 6);
        TransactionId transactionId = new TransactionId(6, LATEST_KERNEL_VERSION, 7, 8, 9);
        var checkpointInfo = ofLogEntry(
                new LogEntryDetachedCheckpointV5_0(
                        LatestVersions.LATEST_KERNEL_VERSION, transactionId, logPosition, 2, storeId, "checkpoint"),
                position,
                positionAfterCheckpoint,
                postReaderPosition,
                null,
                null);

        assertEquals(logPosition, checkpointInfo.transactionLogPosition());
        assertEquals(storeId, checkpointInfo.storeId());
        assertEquals(position, checkpointInfo.checkpointEntryPosition());
        assertEquals(transactionId, checkpointInfo.transactionId());
        assertEquals(positionAfterCheckpoint, checkpointInfo.channelPositionAfterCheckpoint());
        assertEquals(postReaderPosition, checkpointInfo.checkpointFilePostReadPosition());
    }

    private void prepareTestResources() throws IOException, URISyntaxException {
        Path logFilesDir = databaseLayout.getTransactionLogsDirectory();
        FileUtils.copyFileToDirectory(
                Path.of(getClass().getResource("checkpoint.0").toURI()), logFilesDir);
        FileUtils.copyFileToDirectory(
                Path.of(getClass().getResource("neostore.transaction.db.0").toURI()), logFilesDir);
    }
}
