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
package org.neo4j.kernel.impl.transaction.log.entry.v522;

import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.neo4j.internal.helpers.Numbers.safeCastIntToShort;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.DETACHED_CHECK_POINT_V5_0;
import static org.neo4j.storageengine.api.StoreIdSerialization.MAX_STORE_ID_LENGTH;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntrySerializer;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdSerialization;
import org.neo4j.storageengine.api.TransactionId;

public class DetachedCheckpointLogEntrySerializerV5_22 extends LogEntrySerializer<LogEntryDetachedCheckpointV5_22> {
    public static final int RECORD_LENGTH_BYTES = 232;
    public static final int MAX_DESCRIPTION_LENGTH = 75;

    public DetachedCheckpointLogEntrySerializerV5_22() {
        super(DETACHED_CHECK_POINT_V5_0);
    }

    @Override
    public LogEntryDetachedCheckpointV5_22 parse(
            KernelVersion version,
            ReadableChannel channel,
            LogPositionMarker marker,
            CommandReaderFactory commandReaderFactory)
            throws IOException {
        LogPosition checkpointedLogPosition = new LogPosition(channel.getLong(), channel.getLong());
        LogPosition oldestNotCompletedPosition = new LogPosition(channel.getLong(), channel.getLong());
        long checkpointTimeMillis = channel.getLong();
        byte[] storeIdBuffer = new byte[MAX_STORE_ID_LENGTH];
        channel.get(storeIdBuffer, storeIdBuffer.length);
        StoreId storeId = StoreIdSerialization.deserializeWithFixedSize(ByteBuffer.wrap(storeIdBuffer));
        var transactionId = new TransactionId(
                channel.getLong(),
                channel.getLong(),
                KernelVersion.getForVersion(channel.get()),
                channel.getInt(),
                channel.getLong(),
                channel.getLong());
        long appendIndex = channel.getLong();
        short reasonBytesLength = channel.getShort();
        byte[] bytes = new byte[MAX_DESCRIPTION_LENGTH];
        channel.get(bytes, MAX_DESCRIPTION_LENGTH);
        String reason = new String(bytes, 0, reasonBytesLength, UTF_8);
        channel.endChecksumAndValidate();
        return new LogEntryDetachedCheckpointV5_22(
                version,
                transactionId,
                appendIndex,
                oldestNotCompletedPosition,
                checkpointedLogPosition,
                checkpointTimeMillis,
                storeId,
                reason);
    }

    @Override
    public int write(WritableChannel channel, LogEntryDetachedCheckpointV5_22 logEntry) throws IOException {
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

        LogPosition checkpointedLogPosition = logEntry.getCheckpointedLogPosition();
        LogPosition oldestNotCompletedPosition = logEntry.getOldestNotCompletedPosition();
        TransactionId transactionId = logEntry.getTransactionId();
        channel.putLong(checkpointedLogPosition.getLogVersion())
                .putLong(checkpointedLogPosition.getByteOffset())
                .putLong(oldestNotCompletedPosition.getLogVersion())
                .putLong(oldestNotCompletedPosition.getByteOffset())
                .putLong(logEntry.getCheckpointTime())
                .put(storeIdBuffer, storeIdBuffer.length)
                .putLong(transactionId.id())
                .putLong(transactionId.appendIndex())
                .put(transactionId.kernelVersion().version())
                .putInt(transactionId.checksum())
                .putLong(transactionId.commitTimestamp())
                .putLong(transactionId.consensusIndex())
                .putLong(logEntry.getLastAppendIndex())
                .putShort(length)
                .put(descriptionBytes, descriptionBytes.length);
        return channel.putChecksum();
    }
}
