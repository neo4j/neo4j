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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.OtherThread;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.wal.entry.LogEnvelopeHeader;
import org.neo4j.wal.enveloped.InvalidEndOfFileReadException;
import org.neo4j.wal.files.LogFilesBuilder;

/**
 * Tests an issue where writer would append data and sometimes rotate the log to new file. When rotating the log
 * there's an intricate relationship between {@link LogVersionRepository}, creating the file and writing
 * the header. Concurrent readers which scans the log stream will use {@link LogVersionBridge} to seemlessly
 * jump over to new files, where the highest file is dictated by {@link LogVersionRepository#getCurrentLogVersion()}.
 * There was this race where the log version was incremented, the new log file created and reader would get
 * to this new file and try to read the header and fail before the header had been written.
 * <p>
 * This test tries to reproduce this race. It will not produce false negatives, but sometimes false positives
 * since it's non-deterministic.
 */
@Neo4jLayoutExtension
@ExtendWith({LifeExtension.class, OtherThreadExtension.class})
class TransactionLogFileRotateAndReadRaceIT {
    @Inject
    private LifeSupport life;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private OtherThread t2;

    // If any of these limits are reached the test ends, that or if there's a failure of course
    private static final int LIMIT_ROTATIONS = 100;
    private static final int LIMIT_READS = 1_000;

    @Test
    void shouldNotSeeEmptyLogFileWhenReadingTransactionStream() throws Exception {
        assumeFalse(
                LatestVersions.LATEST_LOG_FORMAT.usesSegments(),
                "Disabled for Enveloped log files as they can give spurious errors when reads overlap live writes");
        // GIVEN
        Config cfg = Config.newBuilder()
                .set(
                        GraphDatabaseSettings.neo4j_home,
                        databaseLayout.getNeo4jLayout().homeDirectory())
                .set(GraphDatabaseSettings.preallocate_logical_logs, false)
                .set(GraphDatabaseSettings.logical_log_rotation_threshold, ByteUnit.kibiBytes(128))
                .build();

        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        WALService walService = LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        fs,
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withConfig(cfg)
                .withStoreId(storeId)
                .build();
        life.add(walService);
        LogFile logFile = walService.getLogFile();
        var writer = (TransactionLogWriter) logFile.getTransactionLogWriter();
        LogPositionMarker startPosition = new LogPositionMarker();
        writer.getCurrentPosition(startPosition);

        // WHEN
        AtomicBoolean end = new AtomicBoolean();
        byte[] dataChunk = new byte[100];
        // one thread constantly writing to and rotating the channel
        CountDownLatch startSignal = new CountDownLatch(1);
        Future<Void> writeFuture = t2.execute(() -> {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            startSignal.countDown();
            int rotations = 0;
            while (!end.get()) {
                int bytesToWrite = random.nextInt(1, dataChunk.length);
                var channel = writer.getChannel();
                channel.beginChecksumForWriting();
                channel.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
                channel.putVersion(LatestVersions.LATEST_KERNEL_VERSION.version());
                channel.putInt(bytesToWrite);
                channel.put(dataChunk, bytesToWrite);
                channel.putChecksum();
                if (logFile.rotationNeeded()) {
                    logFile.rotate();
                    // Let's just close the gap to the reader so that it gets closer to the "hot zone"
                    // where the rotation happens.
                    writer.getCurrentPosition(startPosition);
                    if (++rotations > LIMIT_ROTATIONS) {
                        end.set(true);
                    }
                }
            }
            return null;
        });
        assertTrue(startSignal.await(10, SECONDS));
        // one thread reading through the channel
        try {
            int reads = 0;
            while (!end.get()) {
                try (ReadableLogChannel reader = logFile.getReader(startPosition.newPosition())) {
                    deplete(reader);
                }
                if (++reads > LIMIT_READS) {
                    end.set(true);
                }
            }
        } finally {
            writeFuture.get();
        }

        // THEN simply getting here means this was successful
    }

    private static void deplete(ReadableLogChannel reader) {
        byte[] dataChunk = new byte[100];
        try {
            long maxIterations = ByteUnit.mebiBytes(1) / dataChunk.length; // no need to read more
            for (int i = 0; i < maxIterations; i++) {
                reader.beginChecksum();
                assertEquals(LatestVersions.LATEST_KERNEL_VERSION.version(), reader.getVersion());
                int bytesToRead = reader.getInt();
                reader.get(dataChunk, bytesToRead);
                reader.endChecksumAndValidate();
            }
        } catch (ReadPastEndException e) {
            // This is OK, it means we've reached the end
        } catch (InvalidEndOfFileReadException e) {
            // This can happen with envelopes as the reader can find the last envelope to be a BEGIN
            // but can't yet see the follow-on file.
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
