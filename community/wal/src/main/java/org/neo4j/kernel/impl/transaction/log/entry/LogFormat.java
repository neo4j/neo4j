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
package org.neo4j.kernel.impl.transaction.log.entry;

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_VERSION_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogSegments.UNKNOWN_LOG_SEGMENT_SIZE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.util.Preconditions.checkArgument;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdSerialization;

public enum LogFormat {
    /**
     * Total 16 bytes
     * - 8 bytes version
     * - 8 bytes last committed tx id
     */
    V6((byte) 6, 16, KernelVersion.V2_3, KernelVersion.V2_3, UNKNOWN_LOG_SEGMENT_SIZE) {
        @Override
        public LogHeader deserializeHeader(long logVersion, ByteBuffer buffer) {
            long previousCommittedTx = buffer.getLong();
            return new LogHeader(
                    getVersionByte(),
                    logVersion,
                    previousCommittedTx,
                    previousCommittedTx,
                    null,
                    getHeaderSize(),
                    UNKNOWN_LOG_SEGMENT_SIZE,
                    BASE_TX_CHECKSUM,
                    null);
        }

        @Override
        public void serializeHeader(ByteBuffer buffer, LogHeader logHeader) {
            throw new UnsupportedOperationException("Cannot write log format V6");
        }

        @Override
        public LogHeader newHeader(
                long logVersion,
                long lastCommittedTxId,
                long appendIndex,
                StoreId storeId,
                int segmentBlockSize,
                int previousLogFileChecksum,
                KernelVersion kernelVersion) {
            throw new UnsupportedOperationException("Cannot write log format V6");
        }
    },

    /**
     * Total 64 bytes
     * - 8 bytes version
     * - 8 bytes last committed tx id
     * - 40 bytes Legacy Store ID
     * - 8 bytes reserved
     * <pre>
     *   |<-                      LOG_HEADER_SIZE                  ->|
     *   |<-LOG_HEADER_VERSION_SIZE->|                               |
     *   |-----------------------------------------------------------|
     *   |          version          | last tx | store id | reserved |
     *  </pre>
     */
    V7((byte) 7, 64, KernelVersion.V4_2, KernelVersion.V4_4, UNKNOWN_LOG_SEGMENT_SIZE) {
        @Override
        public LogHeader deserializeHeader(long logVersion, ByteBuffer buffer) {
            long previousCommittedTx = buffer.getLong();
            buffer.getLong(); // legacy creation time
            buffer.getLong(); // legacy random
            buffer.getLong(); // legacy store version
            buffer.getLong(); // legacy upgrade time
            buffer.getLong(); // legacy upgrade tx id
            buffer.getLong(); // reserved
            return new LogHeader(
                    getVersionByte(),
                    logVersion,
                    previousCommittedTx,
                    previousCommittedTx,
                    null,
                    getHeaderSize(),
                    UNKNOWN_LOG_SEGMENT_SIZE,
                    BASE_TX_CHECKSUM,
                    null);
        }

        @Override
        public void serializeHeader(ByteBuffer buffer, LogHeader logHeader) {
            throw new UnsupportedOperationException("Cannot write log format V7");
        }

        @Override
        public LogHeader newHeader(
                long logVersion,
                long lastCommittedTxId,
                long appendIndex,
                StoreId storeId,
                int segmentBlockSize,
                int previousLogFileChecksum,
                KernelVersion kernelVersion) {
            throw new UnsupportedOperationException("Cannot write log format V7");
        }
    },

    /**
     * Total 128 bytes
     * - 8 bytes version
     * - 8 bytes last committed tx id
     * - 64 bytes Store ID
     * - 48 bytes reserved
     * <pre>
     *   |<-                      LOG_HEADER_SIZE                  ->|
     *   |<-LOG_HEADER_VERSION_SIZE->|                               |
     *   |-----------------------------------------------------------|
     *   |          version          | last tx | store id | reserved |
     *  </pre>
     */
    V8((byte) 8, 128, KernelVersion.V5_0, KernelVersion.V5_19, UNKNOWN_LOG_SEGMENT_SIZE) {
        @Override
        public LogHeader deserializeHeader(long logVersion, ByteBuffer buffer) throws IOException {
            long previousCommittedTx = buffer.getLong();
            StoreId storeId = StoreIdSerialization.deserializeWithFixedSize(buffer);
            buffer.position(getHeaderSize()); // rest is reserved
            return new LogHeader(
                    getVersionByte(),
                    logVersion,
                    previousCommittedTx,
                    previousCommittedTx,
                    storeId,
                    getHeaderSize(),
                    UNKNOWN_LOG_SEGMENT_SIZE,
                    BASE_TX_CHECKSUM,
                    null);
        }

        @Override
        public void serializeHeader(ByteBuffer buffer, LogHeader logHeader) throws IOException {
            ByteOrder originalOrder = buffer.order();
            try {
                buffer.order(ByteOrder.BIG_ENDIAN);
                buffer.putLong(encodeLogVersion(
                        logHeader.getLogVersion(),
                        logHeader.getLogFormatVersion().getVersionByte()));
                buffer.putLong(logHeader.getLastCommittedTxId());
                StoreIdSerialization.serializeWithFixedSize(logHeader.getStoreId(), buffer);

                // Pad rest with zeroes
                while (buffer.position() < getHeaderSize()) {
                    buffer.put((byte) 0);
                }
            } finally {
                buffer.order(originalOrder);
            }
        }

        @Override
        public LogHeader newHeader(
                long logVersion,
                long lastCommittedTxId,
                long appendIndex,
                StoreId storeId,
                int segmentBlockSize,
                int previousLogFileChecksum,
                KernelVersion kernelVersion) {
            return new LogHeader(
                    getVersionByte(),
                    logVersion,
                    lastCommittedTxId,
                    lastCommittedTxId,
                    storeId,
                    getHeaderSize(),
                    UNKNOWN_LOG_SEGMENT_SIZE,
                    BASE_TX_CHECKSUM,
                    null);
        }
    },

    V9(
            (byte) 9,
            128,
            KernelVersion.V5_20,
            KernelVersion.getLatestVersion(Config.defaults()),
            UNKNOWN_LOG_SEGMENT_SIZE) {
        @Override
        public LogHeader deserializeHeader(long logVersion, ByteBuffer buffer) throws IOException {
            long previousCommittedTx = buffer.getLong();
            long appendIndex = buffer.getLong();
            StoreId storeId = StoreIdSerialization.deserializeWithFixedSize(buffer);
            buffer.position(getHeaderSize()); // rest is reserved
            return new LogHeader(
                    getVersionByte(),
                    logVersion,
                    previousCommittedTx,
                    appendIndex,
                    storeId,
                    getHeaderSize(),
                    UNKNOWN_LOG_SEGMENT_SIZE,
                    BASE_TX_CHECKSUM,
                    null);
        }

        @Override
        public void serializeHeader(ByteBuffer buffer, LogHeader logHeader) throws IOException {
            ByteOrder originalOrder = buffer.order();
            try {
                buffer.order(ByteOrder.BIG_ENDIAN);
                buffer.putLong(encodeLogVersion(
                        logHeader.getLogVersion(),
                        logHeader.getLogFormatVersion().getVersionByte()));
                buffer.putLong(logHeader.getLastCommittedTxId());
                buffer.putLong(logHeader.getLastAppendIndex());
                StoreIdSerialization.serializeWithFixedSize(logHeader.getStoreId(), buffer);

                // Pad rest with zeroes
                while (buffer.position() < getHeaderSize()) {
                    buffer.put((byte) 0);
                }
            } finally {
                buffer.order(originalOrder);
            }
        }

        @Override
        public LogHeader newHeader(
                long logVersion,
                long lastCommittedTxId,
                long appendIndex,
                StoreId storeId,
                int segmentBlockSize,
                int previousLogFileChecksum,
                KernelVersion kernelVersion) {
            return new LogHeader(
                    getVersionByte(),
                    logVersion,
                    lastCommittedTxId,
                    appendIndex,
                    storeId,
                    getHeaderSize(),
                    UNKNOWN_LOG_SEGMENT_SIZE,
                    BASE_TX_CHECKSUM,
                    null);
        }
    },

    /**
     * Total 128 bytes
     * - 8 bytes version
     * - 8 bytes last committed tx id
     * - 8 bytes append index
     * - 64 bytes Store ID
     * - 4 bytes segment block size
     * - 4 bytes previous checksum, i.e. last checksum in the previous file
     * - 1 byte kernel version
     * - 31 bytes reserved
     * <pre>
     *   |<-                      LOG_HEADER_SIZE                                                   ->|
     *   |<-LOG_HEADER_VERSION_SIZE->|                                                                |
     *   |--------------------------------------------------------------------------------------------|
     *   |          version          | last tx | store id | block size | previous checksum | reserved |
     *  </pre>
     */
    V10(
            (byte) 10,
            128,
            KernelVersion.GLORIOUS_FUTURE,
            KernelVersion.GLORIOUS_FUTURE,
            LogSegments.DEFAULT_LOG_SEGMENT_SIZE) {
        @Override
        public LogHeader deserializeHeader(long logVersion, ByteBuffer buffer) throws IOException {
            long previousCommittedTx = buffer.getLong();
            long appendIndex = buffer.getLong();
            StoreId storeId = StoreIdSerialization.deserializeWithFixedSize(buffer);
            int segmentBlockSize = buffer.getInt();
            int previousChecksum = buffer.getInt();
            byte kernelVersion = buffer.get();
            buffer.position(getHeaderSize()); // rest is reserved
            return new LogHeader(
                    getVersionByte(),
                    logVersion,
                    previousCommittedTx,
                    appendIndex,
                    storeId,
                    getHeaderSize(),
                    segmentBlockSize,
                    previousChecksum,
                    KernelVersion.getForVersion(kernelVersion));
        }

        @Override
        public void serializeHeader(ByteBuffer buffer, LogHeader logHeader) throws IOException {
            ByteOrder originalOrder = buffer.order();
            try {
                buffer.order(ByteOrder.BIG_ENDIAN);
                buffer.putLong(encodeLogVersion(
                        logHeader.getLogVersion(),
                        logHeader.getLogFormatVersion().getVersionByte()));
                buffer.putLong(logHeader.getLastCommittedTxId());
                buffer.putLong(logHeader.getLastAppendIndex());
                StoreIdSerialization.serializeWithFixedSize(logHeader.getStoreId(), buffer);
                buffer.putInt(logHeader.getSegmentBlockSize());
                buffer.putInt(logHeader.getPreviousLogFileChecksum());
                buffer.put(logHeader.getKernelVersion().version());

                // Pad rest with zeroes
                while (buffer.position() < logHeader.getStartPosition().getByteOffset()) {
                    buffer.put((byte) 0);
                }
            } finally {
                buffer.order(originalOrder);
            }
        }

        @Override
        public LogHeader newHeader(
                long logVersion,
                long lastCommittedTxId,
                long appendIndex,
                StoreId storeId,
                int segmentBlockSize,
                int previousLogFileChecksum,
                KernelVersion kernelVersion) {
            return new LogHeader(
                    getVersionByte(),
                    logVersion,
                    lastCommittedTxId,
                    appendIndex,
                    storeId,
                    getHeaderSize(),
                    segmentBlockSize,
                    previousLogFileChecksum,
                    kernelVersion);
        }
    };

    public static final int BIGGEST_HEADER;
    static final long LOG_VERSION_BITS = 56;
    static final long LOG_VERSION_MASK = 0x00FF_FFFF_FFFF_FFFFL;
    private static final LogFormat[] BY_VERSION_BYTE;
    private static final EnumMap<KernelVersion, LogFormat> KERNEL_VERSION_TO_LOG_FORMAT =
            new EnumMap<>(KernelVersion.class);

    static {
        int biggestHeader = 0;
        BY_VERSION_BYTE = new LogFormat[Byte.MAX_VALUE];
        for (LogFormat format : LogFormat.values()) {
            BY_VERSION_BYTE[format.versionByte] = format;
            if (biggestHeader < format.headerSize) {
                biggestHeader = format.headerSize;
            }
        }
        BIGGEST_HEADER = biggestHeader;

        buildKernelToFormatMap();
    }

    private final byte versionByte;
    private final int headerSize;
    private final KernelVersion fromKernelVersion;
    private final KernelVersion toKernelVersion;
    private final int defaultSegmentBlockSize;

    LogFormat(byte versionByte, int headerSize, KernelVersion from, KernelVersion to, int defaultSegmentBlockSize) {
        this.versionByte = versionByte;
        this.headerSize = headerSize;
        this.fromKernelVersion = from;
        this.toKernelVersion = to;
        this.defaultSegmentBlockSize = defaultSegmentBlockSize;
    }

    public abstract void serializeHeader(ByteBuffer buffer, LogHeader logHeader) throws IOException;

    public abstract LogHeader deserializeHeader(long logVersion, ByteBuffer buffer) throws IOException;

    public abstract LogHeader newHeader(
            long logVersion,
            long lastCommittedTxId,
            long lastAppendIndex,
            StoreId storeId,
            int segmentBlockSize,
            int previousLogFileChecksum,
            KernelVersion kernelVersion);

    public byte getVersionByte() {
        return versionByte;
    }

    public int getHeaderSize() {
        return headerSize;
    }

    public KernelVersion getFromKernelVersion() {
        return fromKernelVersion;
    }

    public int getDefaultSegmentBlockSize() {
        return defaultSegmentBlockSize;
    }

    public boolean usesSegments() {
        return defaultSegmentBlockSize != UNKNOWN_LOG_SEGMENT_SIZE;
    }

    public static LogHeader parseHeader(ByteBuffer buffer, boolean strict, Path sourceFile) throws IOException {
        if (buffer.remaining() == 0) {
            // Empty file
            return null;
        }
        ByteOrder originalOrder = buffer.order();
        try {
            // Log header uses big endian
            buffer.order(ByteOrder.BIG_ENDIAN);

            if (checkUnderflow(buffer, LOG_HEADER_VERSION_SIZE, strict, sourceFile)) {
                return null;
            }
            long encodedLogVersions = buffer.getLong();
            if (encodedLogVersions == 0) {
                // Since the format version is a non-zero number, we know we are reading a pre-allocated file
                return null;
            }
            byte logFormatVersion = decodeLogFormatVersion(encodedLogVersions);
            long logVersion = decodeLogVersion(encodedLogVersions);

            LogFormat logFormat = BY_VERSION_BYTE[logFormatVersion];
            if (logFormat == null) {
                throw new IOException("Unrecognized transaction log format version: " + logFormatVersion);
            }

            if (checkUnderflow(buffer, logFormat.headerSize - LOG_HEADER_VERSION_SIZE, strict, sourceFile)) {
                return null;
            }

            return logFormat.deserializeHeader(logVersion, buffer);
        } finally {
            buffer.order(originalOrder);
        }
    }

    public static void writeLogHeader(StoreChannel channel, LogHeader logHeader, MemoryTracker memoryTracker)
            throws IOException {
        LogFormat logFormat = logHeader.getLogFormatVersion();

        int headerSize = (int) logHeader.getStartPosition().getByteOffset();
        try (var scopedBuffer = new NativeScopedBuffer(headerSize, ByteOrder.BIG_ENDIAN, memoryTracker)) {
            var buffer = scopedBuffer.getBuffer();
            logFormat.serializeHeader(buffer, logHeader);
            buffer.position(headerSize);
            buffer.flip();
            channel.writeAll(buffer);
            channel.flush();
        }
    }

    private static void buildKernelToFormatMap() {
        LogFormat[] logFormats = LogFormat.values();
        Arrays.sort(logFormats, Comparator.comparingInt(LogFormat::getVersionByte));
        KernelVersion[] kernelVersions = KernelVersion.values();
        Arrays.sort(kernelVersions, Comparator.comparingInt(KernelVersion::version));
        int i = 0;
        for (KernelVersion kernelVersion : kernelVersions) {
            if (kernelVersion == KernelVersion.GLORIOUS_FUTURE) {
                // Handled separately
                continue;
            }
            while (kernelVersion.isGreaterThan(logFormats[i].toKernelVersion)) {
                i++;
            }
            KERNEL_VERSION_TO_LOG_FORMAT.put(kernelVersion, logFormats[i]);
        }
        for (LogFormat logFormat : logFormats) {
            if (logFormat.toKernelVersion == KernelVersion.GLORIOUS_FUTURE) {
                KERNEL_VERSION_TO_LOG_FORMAT.put(KernelVersion.GLORIOUS_FUTURE, logFormat);
            }
        }
    }

    public static LogFormat fromKernelVersion(KernelVersion kernelVersion) {
        return KERNEL_VERSION_TO_LOG_FORMAT.get(kernelVersion);
    }

    public static LogFormat fromByteVersion(byte versionByte) {
        LogFormat logFormat = BY_VERSION_BYTE[versionByte];
        checkArgument(logFormat != null, "Unknown log format byte version: %d".formatted(versionByte));
        return logFormat;
    }

    private static boolean checkUnderflow(
            ByteBuffer buffer, int require, boolean strict, Path fileForAdditionalErrorInformationOrNull)
            throws IncompleteLogHeaderException {
        if (buffer.remaining() < require) {
            if (strict) {
                if (fileForAdditionalErrorInformationOrNull != null) {
                    throw new IncompleteLogHeaderException(
                            fileForAdditionalErrorInformationOrNull, buffer.remaining(), require);
                }
                throw new IncompleteLogHeaderException(buffer.remaining(), require);
            }
            return true;
        }
        return false;
    }

    private static long decodeLogVersion(long encLogVersion) {
        return encLogVersion & LOG_VERSION_MASK;
    }

    private static byte decodeLogFormatVersion(long encLogVersion) {
        return (byte) ((encLogVersion >> LOG_VERSION_BITS) & 0xFF);
    }

    static long encodeLogVersion(long logVersion, long formatVersion) {
        return (logVersion & LOG_VERSION_MASK) | (formatVersion << LOG_VERSION_BITS);
    }
}
