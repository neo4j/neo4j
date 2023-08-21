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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdSerialization;

public enum LogFormat {
    /**
     * Total 16 bytes
     * - 8 bytes version
     * - 8 bytes last committed tx id
     */
    V6((byte) 6, 16, KernelVersion.V2_3, KernelVersion.V2_3, (logVersion, buffer) -> {
        long previousCommittedTx = buffer.getLong();
        return new LogHeader(
                (byte) 6,
                new LogPosition(logVersion, 16),
                previousCommittedTx,
                null,
                UNKNOWN_LOG_SEGMENT_SIZE,
                BASE_TX_CHECKSUM);
    }),

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
    V7((byte) 7, 64, KernelVersion.V4_2, KernelVersion.V4_4, (logVersion, buffer) -> {
        long previousCommittedTx = buffer.getLong();
        buffer.getLong(); // legacy creation time
        buffer.getLong(); // legacy random
        buffer.getLong(); // legacy store version
        buffer.getLong(); // legacy upgrade time
        buffer.getLong(); // legacy upgrade tx id
        buffer.getLong(); // reserved
        return new LogHeader(
                (byte) 7,
                new LogPosition(logVersion, 64),
                previousCommittedTx,
                null,
                UNKNOWN_LOG_SEGMENT_SIZE,
                BASE_TX_CHECKSUM);
    }),

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
    V8((byte) 8, 128, KernelVersion.V5_0, KernelVersion.getLatestVersion(Config.defaults()), (logVersion, buffer) -> {
        long previousCommittedTx = buffer.getLong();
        StoreId storeId = StoreIdSerialization.deserializeWithFixedSize(buffer);
        buffer.getLong(); // reserved
        buffer.getLong(); // reserved
        buffer.getLong(); // reserved
        buffer.getLong(); // reserved
        buffer.getLong(); // reserved
        buffer.getLong(); // reserved
        return new LogHeader(
                (byte) 8,
                new LogPosition(logVersion, 128),
                previousCommittedTx,
                storeId,
                UNKNOWN_LOG_SEGMENT_SIZE,
                BASE_TX_CHECKSUM);
    }),

    /**
     * Total 128 bytes
     * - 8 bytes version
     * - 8 bytes last committed tx id
     * - 64 bytes Store ID
     * - 4 bytes segment block size
     * - 4 bytes previous checksum, i.e. last checksum in the previous file
     * - 40 bytes reserved
     * <pre>
     *   |<-                      LOG_HEADER_SIZE                                                   ->|
     *   |<-LOG_HEADER_VERSION_SIZE->|                                                                |
     *   |--------------------------------------------------------------------------------------------|
     *   |          version          | last tx | store id | block size | previous checksum | reserved |
     *  </pre>
     */
    V9((byte) 9, 128, KernelVersion.GLORIOUS_FUTURE, KernelVersion.GLORIOUS_FUTURE, (logVersion, buffer) -> {
        long previousCommittedTx = buffer.getLong();
        StoreId storeId = StoreIdSerialization.deserializeWithFixedSize(buffer);
        int segmentBlockSize = buffer.getInt();
        int previousChecksum = buffer.getInt();
        buffer.getLong(); // reserved
        buffer.getLong(); // reserved
        buffer.getLong(); // reserved
        buffer.getLong(); // reserved
        buffer.getLong(); // reserved
        return new LogHeader(
                (byte) 9,
                // first block is zero-d (other than the log header itself)
                new LogPosition(logVersion, segmentBlockSize),
                previousCommittedTx,
                storeId,
                segmentBlockSize,
                previousChecksum);
    });

    private static final long LOG_VERSION_BITS = 56;
    private static final long LOG_VERSION_MASK = 0x00FF_FFFF_FFFF_FFFFL;

    private final byte versionByte;
    private final int headerSize;
    private final KernelVersion fromKernelVersion;
    private final KernelVersion toKernelVersion;
    private final LogFormatHeaderParser headerParser;

    LogFormat(
            byte versionByte,
            int headerSize,
            KernelVersion from,
            KernelVersion to,
            LogFormatHeaderParser headerParser) {
        this.versionByte = versionByte;
        this.headerSize = headerSize;
        this.fromKernelVersion = from;
        this.toKernelVersion = to;
        this.headerParser = headerParser;
    }

    public byte getVersionByte() {
        return versionByte;
    }

    public int getHeaderSize() {
        return headerSize;
    }

    public static LogHeader parseHeader(ByteBuffer buffer, boolean strict, Path sourceFile) throws IOException {
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

            return logFormat.headerParser.parse(logVersion, buffer);
        } finally {
            buffer.order(originalOrder);
        }
    }

    /**
     * Current and latest log format version
     */
    public static final byte CURRENT_LOG_FORMAT_VERSION = V8.versionByte;

    /**
     * Current and latest header format byte size.
     */
    public static final int CURRENT_FORMAT_LOG_HEADER_SIZE = V8.headerSize;

    public static final int BIGGEST_HEADER;
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

    private static void buildKernelToFormatMap() {
        LogFormat[] logFormats = LogFormat.values();
        Arrays.sort(logFormats, Comparator.comparingInt(LogFormat::getVersionByte));
        KernelVersion[] kernelVersions = KernelVersion.values();
        Arrays.sort(kernelVersions, Comparator.comparingInt(KernelVersion::version));
        int i = 0;
        for (KernelVersion kernelVersion : kernelVersions) {
            if (kernelVersion == KernelVersion.GLORIOUS_FUTURE) {
                // The future log format has not been invented yet!
                continue;
            }
            while (kernelVersion.isGreaterThan(logFormats[i].toKernelVersion)) {
                i++;
            }
            KERNEL_VERSION_TO_LOG_FORMAT.put(kernelVersion, logFormats[i]);
        }
    }

    public static LogFormat fromKernelVersion(KernelVersion kernelVersion) {
        return KERNEL_VERSION_TO_LOG_FORMAT.get(kernelVersion);
    }

    private interface LogFormatHeaderParser {
        LogHeader parse(long logVersion, ByteBuffer buffer) throws IOException;
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
}
