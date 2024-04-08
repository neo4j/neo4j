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
package org.neo4j.kernel.impl.transaction.log.entry.v50;

import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.neo4j.internal.helpers.Numbers.safeCastIntToShort;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.DETACHED_CHECK_POINT_V5_0;
import static org.neo4j.storageengine.api.StoreIdSerialization.MAX_STORE_ID_LENGTH;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntrySerializer;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdSerialization;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;

/**
 * <table>
 *     <tr>
 *         <th>Bytes</th>
 *         <th>Type</th
 *         <th>Description</th>
 *     </tr>
 *     <tr>
 *         <td>1</td>
 *         <td>byte</td>
 *         <td>version, {@link KernelVersion#version()}</td>
 *     </tr>
 *     <tr>
 *         <td>1</td>
 *         <td>byte</td>
 *         <td>type, {@link LogEntryTypeCodes#DETACHED_CHECK_POINT_V5_0}</td>
 *     </tr>
 *     <tr>
 *         <td>8</td>
 *         <td>long</td>
 *         <td>logFileVersion</td>
 *     </tr>
 *     <tr>
 *         <td>8</td>
 *         <td>long</td>
 *         <td>logFileOffset</td>
 *     </tr>
 *     <tr>
 *         <td>8</td>
 *         <td>long</td>
 *         <td>checkpointTimeMillis</td>
 *     </tr>
 *     <tr>
 *         <td>{@link StoreIdSerialization#MAX_STORE_ID_LENGTH}</td>
 *         <td>{@link StoreId}</td>
 *         <td>storeId</td>
 *     </tr>
 *     <tr>
 *         <td>id(8) + checksum(4) + timestamp(8) = 20</td>
 *         <td>{@link TransactionId}</td>
 *         <td>transactionId with fixed {@link TransactionIdStore#UNKNOWN_CONSENSUS_INDEX}</td>
 *     </tr>
 *     <tr>
 *         <td>2</td>
 *         <td>short</td>
 *         <td>reason length</td>
 *     </tr>
 *     <tr>
 *         <td>{@link #MAX_DESCRIPTION_LENGTH} = 116</td>
 *         <td>{@link String}</td>
 *         <td>reason</td>
 *     </tr>
 *     <tr>
 *         <td>4</td>
 *         <td>int</td>
 *         <td>checksum, {@link CRC32C}</td>
 *     </tr>
 *     <tr>
 *          <td rowspan="3"><strong>Total: 232 bytes</strong></td>
 *     </tr>
 * </table>
 */
public class DetachedCheckpointLogEntrySerializerV5_0 extends LogEntrySerializer<LogEntryDetachedCheckpointV5_0> {
    public static final int RECORD_LENGTH_BYTES = 232;
    public static final int MAX_DESCRIPTION_LENGTH = 116;

    public DetachedCheckpointLogEntrySerializerV5_0() {
        super(LogEntryTypeCodes.DETACHED_CHECK_POINT_V5_0);
    }

    @Override
    public LogEntryDetachedCheckpointV5_0 parse(
            KernelVersion version,
            ReadableChannel channel,
            LogPositionMarker marker,
            CommandReaderFactory commandReaderFactory)
            throws IOException {
        long logVersion = channel.getLong();
        long byteOffset = channel.getLong();
        long checkpointTimeMillis = channel.getLong();
        byte[] storeIdBuffer = new byte[MAX_STORE_ID_LENGTH];
        channel.get(storeIdBuffer, storeIdBuffer.length);
        StoreId storeId = StoreIdSerialization.deserializeWithFixedSize(ByteBuffer.wrap(storeIdBuffer));
        var transactionId = new TransactionId(
                channel.getLong(), version, channel.getInt(), channel.getLong(), UNKNOWN_CONSENSUS_INDEX);
        short reasonBytesLength = channel.getShort();
        byte[] bytes = new byte[MAX_DESCRIPTION_LENGTH];
        channel.get(bytes, MAX_DESCRIPTION_LENGTH);
        String reason = new String(bytes, 0, reasonBytesLength, UTF_8);
        channel.endChecksumAndValidate();
        return new LogEntryDetachedCheckpointV5_0(
                version,
                transactionId,
                new LogPosition(logVersion, byteOffset),
                checkpointTimeMillis,
                storeId,
                reason,
                false);
    }

    @Override
    public int write(WritableChannel channel, LogEntryDetachedCheckpointV5_0 logEntry) throws IOException {
        // Header
        channel.beginChecksumForWriting();
        writeLogEntryHeader(logEntry.kernelVersion(), DETACHED_CHECK_POINT_V5_0, channel);

        // Store id
        byte[] storeIdBuffer = new byte[MAX_STORE_ID_LENGTH];
        StoreIdSerialization.serializeWithFixedSize(logEntry.getStoreId(), ByteBuffer.wrap(storeIdBuffer));

        // Reason
        byte[] reasonBytes = logEntry.getReason().getBytes();
        short length = safeCastIntToShort(min(reasonBytes.length, MAX_DESCRIPTION_LENGTH));
        byte[] descriptionBytes = new byte[MAX_DESCRIPTION_LENGTH];
        System.arraycopy(reasonBytes, 0, descriptionBytes, 0, length);

        LogPosition logPosition = logEntry.getLogPosition();
        TransactionId transactionId = logEntry.getTransactionId();
        channel.putLong(logPosition.getLogVersion())
                .putLong(logPosition.getByteOffset())
                .putLong(logEntry.getCheckpointTime())
                .put(storeIdBuffer, storeIdBuffer.length)
                .putLong(transactionId.id())
                .putInt(transactionId.checksum())
                .putLong(transactionId.commitTimestamp())
                .putShort(length)
                .put(descriptionBytes, descriptionBytes.length);
        return channel.putChecksum();
    }
}
