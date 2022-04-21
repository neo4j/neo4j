/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointInfoFactory.ofLogEntry;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.entry.v50.LogEntryDetachedCheckpointV5_0;
import org.neo4j.kernel.recovery.LogTailExtractor;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.LegacyStoreId;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;

@PageCacheExtension
@Neo4jLayoutExtension
class CheckpointInfoFactoryTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private PageCache pageCache;

    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void checkpointInfoOfDetachedCheckpoint42Entry() throws IOException, URISyntaxException {
        var logPosition = new LogPosition(0, 48820);
        var storeId = new LegacyStoreId(1645458406002L, 3689108786886031620L, 3471768636287762695L);
        LogPosition position = new LogPosition(0, 448);
        LogPosition positionAfterCheckpoint = new LogPosition(0, 640);
        LogPosition postReaderPosition = new LogPosition(0, 640);
        var transactionId = new TransactionId(73, 614900954, 1645458411645L);

        prepareTestResources();

        LogTailMetadata tailMetadata = new LogTailExtractor(
                        fs,
                        pageCache,
                        Config.defaults(),
                        StorageEngineFactory.defaultStorageEngine(),
                        DatabaseTracers.EMPTY)
                .getTailMetadata(databaseLayout, EmptyMemoryTracker.INSTANCE);

        var checkpointInfo = tailMetadata.getLastCheckPoint();

        assertEquals(logPosition, checkpointInfo.getTransactionLogPosition());
        assertEquals(storeId, checkpointInfo.storeId());
        assertEquals(position, checkpointInfo.getCheckpointEntryPosition());
        assertEquals(positionAfterCheckpoint, checkpointInfo.getChannelPositionAfterCheckpoint());
        assertEquals(postReaderPosition, checkpointInfo.getCheckpointFilePostReadPosition());
        assertEquals(transactionId, checkpointInfo.getTransactionId());
    }

    @Test
    void checkpointInfoOfDetachedCheckpoint50Entry() {
        var logPosition = new LogPosition(0, 1);
        var storeId = new LegacyStoreId(3, 4, 5);
        LogPosition position = new LogPosition(1, 2);
        LogPosition positionAfterCheckpoint = new LogPosition(3, 4);
        LogPosition postReaderPosition = new LogPosition(5, 6);
        TransactionId transactionId = new TransactionId(6, 7, 8);
        var checkpointInfo = ofLogEntry(
                new LogEntryDetachedCheckpointV5_0(
                        KernelVersion.LATEST, transactionId, logPosition, 2, storeId, "checkpoint"),
                position,
                positionAfterCheckpoint,
                postReaderPosition,
                null,
                null);

        assertEquals(logPosition, checkpointInfo.getTransactionLogPosition());
        assertEquals(storeId, checkpointInfo.storeId());
        assertEquals(position, checkpointInfo.getCheckpointEntryPosition());
        assertEquals(transactionId, checkpointInfo.getTransactionId());
        assertEquals(positionAfterCheckpoint, checkpointInfo.getChannelPositionAfterCheckpoint());
        assertEquals(postReaderPosition, checkpointInfo.getCheckpointFilePostReadPosition());
    }

    private void prepareTestResources() throws IOException, URISyntaxException {
        Path logFilesDir = databaseLayout.getTransactionLogsDirectory();
        FileUtils.copyFileToDirectory(
                Path.of(getClass().getResource("checkpoint.0").toURI()), logFilesDir);
        FileUtils.copyFileToDirectory(
                Path.of(getClass().getResource("neostore.transaction.db.0").toURI()), logFilesDir);
    }
}
