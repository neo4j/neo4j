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
package org.neo4j.kernel.impl.transaction.log.entry.v57;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.CHUNK_END;

import java.io.IOException;
import java.util.zip.CRC32C;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntrySerializer;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes;
import org.neo4j.storageengine.api.CommandReaderFactory;

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
 *         <td>type, {@link LogEntryTypeCodes#CHUNK_END}</td>
 *     </tr>
 *     <tr>
 *         <td>8</td>
 *         <td>long</td>
 *         <td>transaction id</td>
 *     </tr>
 *     <tr>
 *         <td>8</td>
 *         <td>long</td>
 *         <td>chunk id</td>
 *     </tr>
 *     <tr>
 *         <td>4</td>
 *         <td>int</td>
 *         <td>checksum, {@link CRC32C}</td>
 *     </tr>
 *     <tr>
 *          <td rowspan="3"><strong>Total: 22 bytes</strong></td>
 *     </tr>
 * </table>
 */
public class ChunkEndLogEntrySerializerV5_7 extends LogEntrySerializer<LogEntryChunkEnd> {
    public ChunkEndLogEntrySerializerV5_7() {
        super(LogEntryTypeCodes.CHUNK_END);
    }

    @Override
    public LogEntryChunkEnd parse(
            KernelVersion version,
            ReadableChannel channel,
            LogPositionMarker marker,
            CommandReaderFactory commandReaderFactory)
            throws IOException {
        long txId = channel.getLong();
        long chunkId = channel.getLong();
        int checksum = channel.endChecksumAndValidate();
        return new LogEntryChunkEnd(version, txId, chunkId, checksum);
    }

    @Override
    public int write(WritableChannel channel, LogEntryChunkEnd logEntry) throws IOException {
        writeLogEntryHeader(logEntry.kernelVersion(), CHUNK_END, channel);
        channel.putLong(logEntry.getTransactionId());
        channel.putLong(logEntry.getChunkId());
        return channel.putChecksum();
    }
}
