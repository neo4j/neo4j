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
package org.neo4j.kernel.impl.transaction.log.entry.v42;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;

import java.io.IOException;
import java.util.zip.CRC32C;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.entry.BadLogEntryException;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntrySerializer;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
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
 *         <td>type, {@link LogEntryTypeCodes#TX_START}</td>
 *     </tr>
 *     <tr>
 *         <td>8</td>
 *         <td>long</td>
 *         <td>timeWritten</td>
 *     </tr>
 *     <tr>
 *         <td>8</td>
 *         <td>long</td>
 *         <td>latestCommittedTxWhenStarted</td>
 *     </tr>
 *     <tr>
 *         <td>4</td>
 *         <td>int</td>
 *         <td>previousChecksum</td>
 *     </tr>
 *     <tr>
 *         <td>4</td>
 *         <td>int</td>
 *         <td>additional header length</td>
 *     </tr>
 *     <tr>
 *         <td>-</td>
 *         <td>byte[]</td>
 *         <td>additional header</td>
 *     </tr>
 *     <tr>
 *         <td>4</td>
 *         <td>int</td>
 *         <td>checksum, {@link CRC32C}</td>
 *     </tr>
 *     <tr>
 *          <td rowspan="3"><strong>Total: 30 + (additional header length) bytes</strong></td>
 *     </tr>
 * </table>
 */
public class StartLogEntrySerializerV4_2 extends LogEntrySerializer<LogEntryStart> {
    public StartLogEntrySerializerV4_2() {
        super(LogEntryTypeCodes.TX_START);
    }

    @Override
    public LogEntryStart parse(
            KernelVersion version,
            ReadableChannel channel,
            LogPositionMarker marker,
            CommandReaderFactory commandReaderFactory)
            throws IOException {
        LogPosition position = marker.newPosition();
        long timeWritten = channel.getLong();
        long latestCommittedTxWhenStarted = channel.getLong();
        int previousChecksum = channel.getInt();
        int additionalHeaderLength = channel.getInt();
        if (additionalHeaderLength > LogEntryStart.MAX_ADDITIONAL_HEADER_SIZE) {
            throw new BadLogEntryException("Additional header length limit(" + LogEntryStart.MAX_ADDITIONAL_HEADER_SIZE
                    + ") exceeded. Parsed length is " + additionalHeaderLength);
        }
        byte[] additionalHeader = new byte[additionalHeaderLength];
        channel.get(additionalHeader, additionalHeaderLength);
        return new LogEntryStartV4_2(
                version, timeWritten, latestCommittedTxWhenStarted, previousChecksum, additionalHeader, position);
    }

    @Override
    public int write(WritableChannel channel, LogEntryStart logEntry) throws IOException {
        channel.beginChecksumForWriting();
        writeLogEntryHeader(logEntry.kernelVersion(), TX_START, channel);
        byte[] additionalHeaderData = logEntry.getAdditionalHeader();
        channel.putLong(logEntry.getTimeWritten())
                .putLong(logEntry.getLastCommittedTxWhenTransactionStarted())
                .putInt(logEntry.getPreviousChecksum())
                .putInt(additionalHeaderData.length)
                .put(additionalHeaderData, additionalHeaderData.length);
        return NO_RETURN_VALUE;
    }
}
