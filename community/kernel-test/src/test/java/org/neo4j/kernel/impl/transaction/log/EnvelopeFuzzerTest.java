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

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.kernel.impl.transaction.log.EnvelopeWriteChannel.START_INDEX;
import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.writeLogHeader;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.LogVersionRepository.INITIAL_LOG_VERSION;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.internal.helpers.MathUtil;
import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.internal.nativeimpl.NativeCallResult;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFileChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvents;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
class EnvelopeFuzzerTest {
    private static final long ROTATION_PERIOD = 42L;

    /**
     * Setting to true will print the random sequence, very useful when trying to figure out why something failed.
     */
    private static final boolean PRINT_SEQUENCE = false;

    @Inject
    private RandomSupport random;

    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDirectory;

    @Disabled("Will need a LogFormat V10 to function")
    @Test
    void randomWritesAndReads() throws IOException {
        int segmentSize = 1 << random.intBetween(log2(128), log2(kibiBytes(256))); // Between 128b to 256kb
        int bufferSize =
                segmentSize * random.intBetween(1, (int) (mebiBytes(4) / kibiBytes(256))); // Between 128b and 4mb
        int rotationSize =
                (int) MathUtil.roundUp(mebiBytes(random.intBetween(16, 32)), bufferSize); // Between 16mb and 32mb
        int initialChecksum = random.nextInt();
        boolean preAllocate = random.nextBoolean();

        List<DataStep> sequence = new ArrayList<>();
        generateRandomInteractions(sequence);

        // Create first file and write header
        PhysicalLogVersionedStoreChannel storeChannel = storeChannel(0, preAllocate, rotationSize);
        LogHeader logHeader = new LogHeader(
                LogFormat.V9,
                INITIAL_LOG_VERSION,
                BASE_TX_ID,
                StoreId.UNKNOWN,
                segmentSize,
                initialChecksum,
                LATEST_KERNEL_VERSION);
        writeLogHeader(storeChannel, logHeader, INSTANCE);
        storeChannel.position(segmentSize);

        // Write random data
        LogRotationForChannel logRotation = logRotation(storeChannel, segmentSize, preAllocate, rotationSize);
        try (EnvelopeWriteChannel envelopeWriteChannel = new EnvelopeWriteChannel(
                storeChannel,
                buffer(bufferSize),
                segmentSize,
                initialChecksum,
                START_INDEX,
                DatabaseTracer.NULL,
                logRotation)) {
            logRotation.bindWriteChannel(envelopeWriteChannel);
            envelopeWriteChannel.putVersion(LATEST_KERNEL_VERSION.version());

            for (int i = 0; i < sequence.size(); i++) {
                DataStep dataStep = sequence.get(i);
                if (PRINT_SEQUENCE) {
                    System.out.printf("w %05d: %s%n", i, dataStep);
                }
                dataStep.write(envelopeWriteChannel);
            }
        }

        // Read back everything and validate
        storeChannel = storeChannel(0, false, 0);
        try (var envelopeReadChannel = new EnvelopeReadChannel(
                storeChannel, segmentSize, simpleLogVersionBridge(), EmptyMemoryTracker.INSTANCE, false)) {

            for (int i = 0; i < sequence.size(); i++) {
                DataStep dataStep = sequence.get(i);
                if (PRINT_SEQUENCE) {
                    System.out.printf("r %05d: %s%n", i, dataStep);
                }
                dataStep.readAndValidate(envelopeReadChannel);
            }
            assertThatThrownBy(envelopeReadChannel::get)
                    .as("Should not contain more data")
                    .isInstanceOf(ReadPastEndException.class);
        }
    }

    private void generateRandomInteractions(List<DataStep> sequence) {
        for (int i = 0; i < 1000; i++) {
            int dist = random.intBetween(0, 100);
            if (dist < 15) { // 15%
                sequence.add(new ByteDataStep((byte) random.nextInt()));
            } else if (dist < 30) { // 15%
                sequence.add(new IntDataStep(random.nextInt()));
            } else if (dist < 45) { // 15%
                sequence.add(new ShortDataStep((short) random.nextInt()));
            } else if (dist < 60) { // 15%
                sequence.add(new LongDataStep(random.nextLong()));
            } else if (dist < 75) { // 15%
                sequence.add(new FloatDataStep(random.nextFloat()));
            } else if (dist < 90) { // 15%
                sequence.add(new DoubleDataStep(random.nextDouble()));
            } else if (dist < 95) { // 3%
                sequence.add(new ByteArrayDataStep(random.nextBytes(new byte[random.nextInt((int) mebiBytes(1))])));
            } else if (dist < 98) { // 2%
                sequence.add(new ByteBufferDataStep(random.nextBytes(new byte[random.nextInt((int) mebiBytes(1))])));
            } else { // 5%
                completeEntry(sequence);
            }
        }
        completeEntry(sequence);
    }

    private static void completeEntry(List<DataStep> sequence) {
        if (sequence.size() > 0) {
            DataStep previousStep = sequence.get(sequence.size() - 1);
            if (!(previousStep instanceof EndEntryDataStep)) {
                sequence.add(new EndEntryDataStep());
            }
        }
    }

    private int log2(long value) {
        return (int) (Math.log(value) / Math.log(2));
    }

    private PhysicalLogVersionedStoreChannel storeChannel(long version, boolean preAllocate, long fileSize)
            throws IOException {
        final var logPath = logPath(version);

        boolean fileExist = fileSystem.fileExists(logPath);
        StoreFileChannel channel = fileSystem.write(logPath);
        if (!fileExist && preAllocate) {
            int fileDescriptor = fileSystem.getFileDescriptor(channel);
            NativeAccess nativeAccess = NativeAccessProvider.getNativeAccess();
            NativeCallResult result = nativeAccess.tryPreallocateSpace(fileDescriptor, fileSize);
            assertThat(result.isError()).isFalse();
        }
        return new PhysicalLogVersionedStoreChannel(
                channel,
                version,
                LATEST_LOG_FORMAT,
                logPath,
                mock(LogFileChannelNativeAccessor.class),
                DatabaseTracer.NULL);
    }

    private Path logPath(long version) {
        return testDirectory.homePath().resolve("log." + version);
    }

    private static HeapScopedBuffer buffer(int segmentSize) {
        return new HeapScopedBuffer(segmentSize, LITTLE_ENDIAN, INSTANCE);
    }

    private LogRotationForChannel logRotation(
            LogVersionedStoreChannel initialChannel, int segmentSize, boolean preAllocate, long maxFileSize) {
        final var currentVersion = new MutableInt(initialChannel.getLogVersion());

        // this is to mimic the behaviour in TransactionLogFile/DetachedCheckpointAppender where the writer
        // manages the updates to the channel on a rotation
        return new LogRotationForChannel() {
            private EnvelopeWriteChannel writeChannel;

            @Override
            public void bindWriteChannel(EnvelopeWriteChannel channel) {
                writeChannel = channel;
            }

            @Override
            public void rotateLogFile(LogRotateEvents logRotateEvents) throws IOException {
                try (var event = logRotateEvents.beginLogRotate()) {
                    final var logChannel = storeChannel(currentVersion.incrementAndGet(), preAllocate, maxFileSize);
                    int previousChecksum = writeChannel.currentChecksum();
                    LogHeader logHeader = new LogHeader(
                            LogFormat.V9,
                            currentVersion.intValue(),
                            BASE_TX_ID,
                            StoreId.UNKNOWN,
                            segmentSize,
                            previousChecksum,
                            LATEST_KERNEL_VERSION);
                    writeLogHeader(logChannel, logHeader, INSTANCE);
                    logChannel.position(segmentSize);

                    writeChannel.setChannel(logChannel);
                    event.rotationCompleted(ROTATION_PERIOD);
                }
            }

            @Override
            public long rotationSize() {
                return maxFileSize;
            }

            @Override
            public boolean rotateLogIfNeeded(LogRotateEvents logRotateEvents) {
                throw new UnsupportedOperationException("envelope channel rotation checks are done internally");
            }

            @Override
            public boolean batchedRotateLogIfNeeded(LogRotateEvents logRotateEvents, long lastTransactionId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean locklessRotateLogIfNeeded(LogRotateEvents logRotateEvents) {
                return rotateLogIfNeeded(logRotateEvents);
            }
        };
    }

    private LogVersionBridge simpleLogVersionBridge() {
        return new LogVersionBridge() {
            int lsn = 1;

            @Override
            public LogVersionedStoreChannel next(LogVersionedStoreChannel previousChannel, boolean raw)
                    throws IOException {
                PhysicalLogVersionedStoreChannel next = storeChannel(lsn++, false, 0);
                previousChannel.close();
                return next;
            }
        };
    }

    private interface LogRotationForChannel extends LogRotation {
        void bindWriteChannel(EnvelopeWriteChannel channel);
    }

    interface DataStep {
        void write(EnvelopeWriteChannel channel) throws IOException;

        void readAndValidate(EnvelopeReadChannel channel) throws IOException;
    }

    private static class ByteDataStep implements DataStep {
        private final byte value;

        private ByteDataStep(byte value) {
            this.value = value;
        }

        @Override
        public void write(EnvelopeWriteChannel channel) throws IOException {
            channel.put(value);
        }

        @Override
        public void readAndValidate(EnvelopeReadChannel channel) throws IOException {
            byte actual = channel.get();
            assertThat(actual).isEqualTo(value);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + value + ")";
        }
    }

    private static class IntDataStep implements DataStep {
        private final int value;

        private IntDataStep(int value) {
            this.value = value;
        }

        @Override
        public void write(EnvelopeWriteChannel channel) throws IOException {
            channel.putInt(value);
        }

        @Override
        public void readAndValidate(EnvelopeReadChannel channel) throws IOException {
            int actual = channel.getInt();
            assertThat(actual).isEqualTo(value);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + value + ")";
        }
    }

    private static class ShortDataStep implements DataStep {
        private final short value;

        private ShortDataStep(short value) {
            this.value = value;
        }

        @Override
        public void write(EnvelopeWriteChannel channel) throws IOException {
            channel.putShort(value);
        }

        @Override
        public void readAndValidate(EnvelopeReadChannel channel) throws IOException {
            short actual = channel.getShort();
            assertThat(actual).isEqualTo(value);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + value + ")";
        }
    }

    private static class LongDataStep implements DataStep {
        private final long value;

        private LongDataStep(long value) {
            this.value = value;
        }

        @Override
        public void write(EnvelopeWriteChannel channel) throws IOException {
            channel.putLong(value);
        }

        @Override
        public void readAndValidate(EnvelopeReadChannel channel) throws IOException {
            long actual = channel.getLong();
            assertThat(actual).isEqualTo(value);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + value + ")";
        }
    }

    private static class FloatDataStep implements DataStep {
        private final float value;

        private FloatDataStep(float value) {
            this.value = value;
        }

        @Override
        public void write(EnvelopeWriteChannel channel) throws IOException {
            channel.putFloat(value);
        }

        @Override
        public void readAndValidate(EnvelopeReadChannel channel) throws IOException {
            float actual = channel.getFloat();
            assertThat(actual).isEqualTo(value);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + value + ")";
        }
    }

    private static class DoubleDataStep implements DataStep {
        private final double value;

        private DoubleDataStep(double value) {
            this.value = value;
        }

        @Override
        public void write(EnvelopeWriteChannel channel) throws IOException {
            channel.putDouble(value);
        }

        @Override
        public void readAndValidate(EnvelopeReadChannel channel) throws IOException {
            double actual = channel.getDouble();
            assertThat(actual).isEqualTo(value);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + value + ")";
        }
    }

    private static class ByteArrayDataStep implements DataStep {
        private final byte[] value;

        private ByteArrayDataStep(byte[] value) {
            this.value = value;
        }

        @Override
        public void write(EnvelopeWriteChannel channel) throws IOException {
            channel.put(value, value.length);
        }

        @Override
        public void readAndValidate(EnvelopeReadChannel channel) throws IOException {
            byte[] actual = new byte[value.length];
            channel.get(actual, actual.length);
            assertThat(actual).isEqualTo(value);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(size: " + value.length + ")";
        }
    }

    private static class ByteBufferDataStep implements DataStep {
        private final ByteBuffer value;

        private ByteBufferDataStep(byte[] value) {
            this.value = ByteBuffer.wrap(value).order(LITTLE_ENDIAN);
        }

        @Override
        public void write(EnvelopeWriteChannel channel) throws IOException {
            channel.putAll(value);
        }

        @Override
        public void readAndValidate(EnvelopeReadChannel channel) throws IOException {
            ByteBuffer buffer = ByteBuffer.wrap(new byte[value.capacity()]);
            channel.read(buffer);
            buffer.flip();
            assertThat(buffer.array()).containsExactly(value.array());
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(size: " + value.capacity() + ")";
        }
    }

    private static class EndEntryDataStep implements DataStep {
        private int value;

        @Override
        public void write(EnvelopeWriteChannel channel) throws IOException {
            channel.endCurrentEntry();
            value = channel.currentChecksum();
        }

        @Override
        public void readAndValidate(EnvelopeReadChannel channel) {
            double actual = channel.endChecksumAndValidate();
            assertThat(actual).isEqualTo(value);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
    }
}
