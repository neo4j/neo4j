/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.entry.v42;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import org.neo4j.io.fs.ReadableChecksumChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryParser;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StoreId;

public class DetachedCheckpointLogEntryParserV4_2 extends LogEntryParser {
    public static final int MAX_DESCRIPTION_LENGTH = 120;

    public DetachedCheckpointLogEntryParserV4_2() {
        super(LogEntryTypeCodes.DETACHED_CHECK_POINT);
    }

    @Override
    public LogEntry parse(
            KernelVersion version,
            ReadableChecksumChannel channel,
            LogPositionMarker marker,
            CommandReaderFactory commandReaderFactory)
            throws IOException {
        long logVersion = channel.getLong();
        long byteOffset = channel.getLong();
        long checkpointTimeMillis = channel.getLong();
        StoreId storeId = constructStoreIdFromLegacyInfo(channel.getLong(), channel.getLong(), channel.getLong());
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

    private StoreId constructStoreIdFromLegacyInfo(long creationTime, long randomId, long storeVersion) {
        return new StoreId(creationTime, randomId, "legacy", "legacy", 1, 1);
    }
}
