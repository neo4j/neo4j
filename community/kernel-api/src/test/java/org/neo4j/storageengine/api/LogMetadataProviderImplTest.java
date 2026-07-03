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
package org.neo4j.storageengine.api;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.KernelVersion.DEFAULT_BOOTSTRAP_VERSION;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.test.Race.throwing;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.EmptyLogTailLogVersionsMetadata;
import org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;

@RandomSupportExtension
class LogMetadataProviderImplTest {

    @Inject
    RandomSupport random;

    @Test
    void testRecordTransactionClosed() {
        // GIVEN
        long version = 1L;
        long byteOffset = 777L;
        int checksum = 5252;
        long timestamp = 1234;
        long consensusIndex = 12345;
        long transactionId;
        LogMetadataProviderImpl metadataProvider = newMetadataProvider(LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);
        transactionId = metadataProvider.getHighestGapFreeClosedTransactionId() + 1;

        // WHEN
        metadataProvider.transactionClosed(
                transactionId, 9, DEFAULT_BOOTSTRAP_VERSION, version, byteOffset, checksum, timestamp, consensusIndex);
        // long[] with the highest offered gap-free number and its meta data.
        var closedTransaction = metadataProvider.getHighestGapFreeClosedTransaction();

        // EXPECT
        LogPosition logPosition = closedTransaction.logPosition();
        assertThat(logPosition.getLogVersion()).isEqualTo(version);
        assertThat(logPosition.getByteOffset()).isEqualTo(byteOffset);

        var tail = new EmptyLogTailLogVersionsMetadata() {
            @Override
            public LogPosition getLastTransactionLogPosition() {
                return new LogPosition(version, byteOffset);
            }

            @Override
            public TransactionId getLastCommittedTransaction() {
                return new TransactionId(
                        transactionId,
                        transactionId + 2,
                        DEFAULT_BOOTSTRAP_VERSION,
                        checksum,
                        timestamp,
                        consensusIndex);
            }
        };

        metadataProvider = newMetadataProvider(tail);
        var lastClosedTransaction = metadataProvider.getHighestGapFreeClosedTransaction();
        logPosition = lastClosedTransaction.logPosition();
        assertThat(logPosition.getLogVersion()).isEqualTo(version);
        assertThat(logPosition.getByteOffset()).isEqualTo(byteOffset);
    }

    @Test
    void incrementAndGetVersionMustBeAtomic() throws Throwable {
        LogMetadataProviderImpl store = newMetadataProvider(LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);
        long initialVersion = store.incrementAndGetVersion();
        int threads = Runtime.getRuntime().availableProcessors();
        int iterations = 500;
        Race race = new Race();
        race.addContestants(threads, () -> {
            for (int i = 0; i < iterations; i++) {
                store.incrementAndGetVersion();
            }
        });
        race.go();
        assertThat(store.incrementAndGetVersion()).isEqualTo(initialVersion + (threads * iterations) + 1);
    }

    @Test
    void transactionCommittedMustBeAtomic() throws Throwable {
        LogMetadataProviderImpl store = newMetadataProvider();
        store.transactionCommitted(2, 3, DEFAULT_BOOTSTRAP_VERSION, 2, 2, 2);
        AtomicLong writeCount = new AtomicLong();
        AtomicLong fileReadCount = new AtomicLong();
        AtomicLong apiReadCount = new AtomicLong();
        int upperLimit = 10_000;
        int lowerLimit = 100;
        long endTime = currentTimeMillis() + SECONDS.toMillis(10);

        Race race = new Race();
        race.withEndCondition(() -> writeCount.get() >= upperLimit
                && fileReadCount.get() >= upperLimit
                && apiReadCount.get() >= upperLimit);
        race.withEndCondition(() -> writeCount.get() >= lowerLimit
                && fileReadCount.get() >= lowerLimit
                && apiReadCount.get() >= lowerLimit
                && currentTimeMillis() >= endTime);
        race.addContestants(3, () -> {
            long count = writeCount.incrementAndGet();
            store.transactionCommitted(count, count + 1, DEFAULT_BOOTSTRAP_VERSION, (int) count, count, count);
        });

        race.addContestants(3, throwing(() -> {
            TransactionId transactionId = store.getLastCommittedTransaction();
            long id = transactionId.id();
            long checksum = transactionId.checksum();
            assertIdEqualsChecksum(id, checksum, "file");
            fileReadCount.incrementAndGet();
        }));

        race.addContestants(3, () -> {
            TransactionId transaction = store.getLastCommittedTransaction();
            assertIdEqualsChecksum(transaction.id(), transaction.checksum(), "API");
            apiReadCount.incrementAndGet();
        });

        race.go();
    }

    @Test
    void transactionClosedMustBeAtomic() throws Throwable {
        LogMetadataProviderImpl store = newMetadataProvider();
        int initialValue = 2;
        store.transactionClosed(
                initialValue,
                initialValue,
                DEFAULT_BOOTSTRAP_VERSION,
                initialValue,
                initialValue,
                initialValue,
                initialValue,
                initialValue);
        AtomicLong writeCount = new AtomicLong();
        AtomicLong fileReadCount = new AtomicLong();
        AtomicLong apiReadCount = new AtomicLong();
        int upperLimit = 10_000;
        int lowerLimit = 100;
        long endTime = currentTimeMillis() + SECONDS.toMillis(10);

        Race race = new Race();
        race.withEndCondition(() -> writeCount.get() >= upperLimit
                && fileReadCount.get() >= upperLimit
                && apiReadCount.get() >= upperLimit);
        race.withEndCondition(() -> writeCount.get() >= lowerLimit
                && fileReadCount.get() >= lowerLimit
                && apiReadCount.get() >= lowerLimit
                && currentTimeMillis() >= endTime);
        race.addContestants(3, () -> {
            long count = writeCount.incrementAndGet();
            store.transactionCommitted(count, count + 1, DEFAULT_BOOTSTRAP_VERSION, (int) count, count, count);
        });

        race.addContestants(3, throwing(() -> {
            LogPosition logPosition = store.getHighestGapFreeClosedTransaction().logPosition();
            long logVersion = logPosition.getLogVersion();
            long byteOffset = logPosition.getByteOffset();
            assertLogVersionEqualsByteOffset(logVersion, byteOffset, "file");
            fileReadCount.incrementAndGet();
        }));

        race.addContestants(3, () -> {
            var transaction = store.getHighestGapFreeClosedTransaction();
            assertLogVersionEqualsByteOffset(
                    transaction.transactionId().id(), transaction.logPosition().getLogVersion(), "API");
            apiReadCount.incrementAndGet();
        });
        race.go();
    }

    @Test
    void lastTxCommitTimestampShouldBeBaseInNewStore() {
        LogMetadataProviderImpl metadataProvider = newMetadataProvider();
        long timestamp = metadataProvider.getLastCommittedTransaction().commitTimestamp();
        assertThat(timestamp).isEqualTo(TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP);
    }

    @Test
    void accessCheckpointLogVersion() {
        LogMetadataProviderImpl dataStore = newMetadataProvider();
        assertThat(dataStore.getCheckpointLogVersion()).isEqualTo(0);
        assertThat(dataStore.incrementAndGetCheckpointLogVersion()).isEqualTo(1);
        assertThat(dataStore.incrementAndGetCheckpointLogVersion()).isEqualTo(2);
        assertThat(dataStore.incrementAndGetCheckpointLogVersion()).isEqualTo(3);
        assertThat(dataStore.incrementAndGetCheckpointLogVersion()).isEqualTo(4);
        assertThat(dataStore.incrementAndGetCheckpointLogVersion()).isEqualTo(5);
        assertThat(dataStore.getCheckpointLogVersion()).isEqualTo(5);
        assertThat(dataStore.getCurrentLogVersion()).isEqualTo(0);
    }

    @Test
    void checkSetCheckpointLogVersion() {
        LogMetadataProviderImpl dataStore = newMetadataProvider();
        assertThat(dataStore.getCheckpointLogVersion()).isEqualTo(0);
        dataStore.setCheckpointLogVersion(123);
        assertThat(dataStore.getCheckpointLogVersion()).isEqualTo(123);

        dataStore.setCheckpointLogVersion(321);
        assertThat(dataStore.getCheckpointLogVersion()).isEqualTo(321);
        assertThat(dataStore.getCurrentLogVersion()).isEqualTo(0);
    }

    @Test
    void setLastClosedTransactionOverridesLastClosedTransactionInformation() {
        LogMetadataProviderImpl metadataProvider = newMetadataProvider();
        metadataProvider.resetLastClosedTransaction(3, 9, DEFAULT_BOOTSTRAP_VERSION, 4, 5, 6, 7, 8);

        assertThat(metadataProvider.getHighestGapFreeClosedTransactionId()).isEqualTo(3L);
        assertThat(metadataProvider.getHighestGapFreeClosedTransaction())
                .isEqualTo(new ClosedTransactionMetadata(
                        new TransactionId(3, 9, DEFAULT_BOOTSTRAP_VERSION, 6, 7, 8), new LogPosition(4, 5)));
    }

    @Test
    void shouldSetHighestTransactionIdWhenNeeded() {
        // GIVEN
        LogMetadataProviderImpl metadataProvider = newMetadataProvider();

        metadataProvider.setLastCommittedAndClosedTransactionId(
                40,
                41,
                DEFAULT_BOOTSTRAP_VERSION,
                4444,
                BASE_TX_COMMIT_TIMESTAMP,
                7,
                LogFormat.V9.getHeaderSize(),
                0,
                44);

        // WHEN
        metadataProvider.transactionCommitted(42, 43, DEFAULT_BOOTSTRAP_VERSION, 6666, BASE_TX_COMMIT_TIMESTAMP, 8);

        // THEN
        assertThat(metadataProvider.getLastCommittedTransaction())
                .isEqualTo(new TransactionId(42, 43, DEFAULT_BOOTSTRAP_VERSION, 6666, BASE_TX_COMMIT_TIMESTAMP, 8));
        assertThat(metadataProvider.getHighestGapFreeClosedTransaction())
                .isEqualTo(new ClosedTransactionMetadata(
                        new TransactionId(40, 41, DEFAULT_BOOTSTRAP_VERSION, 4444, BASE_TX_COMMIT_TIMESTAMP, 7),
                        new LogPosition(0, LogFormat.V9.getHeaderSize())));
    }

    @Test
    void shouldNotSetHighestTransactionIdWhenNeeded() {
        // GIVEN
        LogMetadataProviderImpl metadataProvider = newMetadataProvider();
        metadataProvider.setLastCommittedAndClosedTransactionId(
                40,
                41,
                DEFAULT_BOOTSTRAP_VERSION,
                4444,
                BASE_TX_COMMIT_TIMESTAMP,
                8,
                LogFormat.V9.getHeaderSize(),
                0,
                44);

        // WHEN
        metadataProvider.transactionCommitted(39, 40, DEFAULT_BOOTSTRAP_VERSION, 3333, BASE_TX_COMMIT_TIMESTAMP, 9);

        // THEN
        assertThat(metadataProvider.getLastCommittedTransaction())
                .isEqualTo(new TransactionId(40, 41, DEFAULT_BOOTSTRAP_VERSION, 4444, BASE_TX_COMMIT_TIMESTAMP, 8));
        assertThat(metadataProvider.getHighestGapFreeClosedTransaction())
                .isEqualTo(new ClosedTransactionMetadata(
                        new TransactionId(40, 41, DEFAULT_BOOTSTRAP_VERSION, 4444, BASE_TX_COMMIT_TIMESTAMP, 8),
                        new LogPosition(0, LogFormat.V9.getHeaderSize())));
    }

    @Test
    void shouldProvideGivenKernelVersion() {
        final var kernelVersion = random.among(KernelVersion.VERSIONS);
        LogMetadataProviderImpl logMetadataProvider =
                new LogMetadataProviderImpl(LogTailLogVersionsMetadata.EMPTY_LOG_TAIL, LogFormat.V10, kernelVersion);

        assertThat(logMetadataProvider.kernelVersion())
                .as("provided kernel version")
                .isEqualTo(kernelVersion);
    }

    @Test
    void shouldProvideKernelVersionFromLogTailMetadata() {
        final var logTailMetadata = randomLogTailMetadata();
        final var kernelVersionProvider = new LogMetadataProviderImpl(logTailMetadata);

        assertThat(kernelVersionProvider.kernelVersion())
                .as("provided kernel version")
                .isEqualTo(logTailMetadata.kernelVersion());
    }

    @Test
    void shouldSetGivenKernelVersion() {
        final var kernelVersions = random.ints(2, 0, KernelVersion.VERSIONS.size())
                .mapToObj(KernelVersion.VERSIONS::get)
                .sorted()
                .toList();
        final var initialKernelVersion = kernelVersions.get(0);
        final var kernelVersion = kernelVersions.get(1);

        final var kernelVersionRepository = new LogMetadataProviderImpl(
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL, LogFormat.V10, initialKernelVersion);

        kernelVersionRepository.setKernelVersion(kernelVersion);
        assertThat(kernelVersionRepository.kernelVersion())
                .as("provided kernel version")
                .isEqualTo(kernelVersion);
    }

    private LogTailMetadata randomLogTailMetadata() {
        return new EmptyLogTailMetadata(Config.defaults()) {
            private final KernelVersion kernelVersion = random.among(KernelVersion.VERSIONS);

            @Override
            public KernelVersion kernelVersion() {
                return kernelVersion;
            }
        };
    }

    private static void assertLogVersionEqualsByteOffset(long logVersion, long byteOffset, String source) {
        if (logVersion != byteOffset) {
            throw new AssertionError("logVersion (" + logVersion + ") and byteOffset (" + byteOffset + ") from "
                    + source + " should be identical");
        }
    }

    private static void assertIdEqualsChecksum(long id, long checksum, String source) {
        if (id != checksum) {
            throw new AssertionError(
                    "id (" + id + ") and checksum (" + checksum + ") from " + source + " should be identical");
        }
    }

    private LogMetadataProviderImpl newMetadataProvider(LogTailLogVersionsMetadata logTail) {
        Config config = Config.defaults();
        KernelVersion kernelVersion = KernelVersion.getLatestVersion(config);
        return new LogMetadataProviderImpl(
                logTail, LogFormat.fromConfigAndKernelVersion(config, kernelVersion), kernelVersion);
    }

    private LogMetadataProviderImpl newMetadataProvider() {
        return newMetadataProvider(LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);
    }
}
