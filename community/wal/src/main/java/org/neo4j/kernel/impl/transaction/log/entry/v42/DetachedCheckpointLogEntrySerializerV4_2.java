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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
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
 *         <td>type, {@link LogEntryTypeCodes#DETACHED_CHECK_POINT}</td>
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
 *         <td>40</td>
 *         <td>5 longs</td>
 *         <td>legacy store id</td>
 *     </tr>
 *     <tr>
 *         <td>2</td>
 *         <td>short</td>
 *         <td>reason length</td>
 *     </tr>
 *     <tr>
 *         <td>{@link #MAX_DESCRIPTION_LENGTH} = 120</td>
 *         <td>{@link String}</td>
 *         <td>reason</td>
 *     </tr>
 *     <tr>
 *         <td>4</td>
 *         <td>int</td>
 *         <td>checksum, {@link CRC32C}</td>
 *     </tr>
 *     <tr>
 *          <td rowspan="3"><strong>Total: 192 bytes</strong></td>
 *     </tr>
 * </table>
 */
public class DetachedCheckpointLogEntrySerializerV4_2 extends LogEntrySerializer<LogEntryDetachedCheckpointV4_2> {
    public static final int MAX_DESCRIPTION_LENGTH = 120;

    public DetachedCheckpointLogEntrySerializerV4_2() {
        super(LogEntryTypeCodes.DETACHED_CHECK_POINT);
    }

    @Override
    public LogEntryDetachedCheckpointV4_2 parse(
            KernelVersion version,
            ReadableChannel channel,
            LogPositionMarker marker,
            CommandReaderFactory commandReaderFactory)
            throws IOException {
        long logVersion = channel.getLong();
        long byteOffset = channel.getLong();
        long checkpointTimeMillis = channel.getLong();
        StoreId storeId = constructStoreIdFromLegacyInfo(channel.getLong(), channel.getLong());
        channel.getLong(); // legacy storeVersion
        channel.getLong(); // legacy upgrade time
        channel.getLong(); // legacy upgrade tx id
        short reasonBytesLength = channel.getShort();
        byte[] bytes = new byte[MAX_DESCRIPTION_LENGTH];
        channel.get(bytes, MAX_DESCRIPTION_LENGTH);
        String reason = new String(bytes, 0, reasonBytesLength, UTF_8);
        channel.endChecksumAndValidate();
        return new LogEntryDetachedCheckpointV4_2(
                version, new LogPosition(logVersion, byteOffset), checkpointTimeMillis, storeId, reason);
    }

    @Override
    public int write(WritableChannel channel, LogEntryDetachedCheckpointV4_2 logEntry) throws IOException {
        throw new UnsupportedOperationException("Unable to write old detached checkpoint log entry.");
    }

    private StoreId constructStoreIdFromLegacyInfo(long creationTime, long randomId) {
        return new StoreId(creationTime, randomId, "legacy", "legacy", 1, 1);
    }
}
