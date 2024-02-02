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
package org.neo4j.kernel.api.database.transaction;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.LogVersionRepository;

/**
 * @param startAppendIndex first append index in this channel.
 * @param kernelVersion    kernel version associated with the first transaction ID in this channel.
 * @param channel          channel containing the transaction data.
 * @param startOffset      offset the data starts on in the channel (only used to send over the wire, the channel is positioned already).
 * @param endOffset        a known transaction-ending aligned offset for this channel.
 * @param lastAppendIndex  last append index in this channel.
 * @param previousChecksum previous checksum if at beginning of a file,
 *                         or {@link LogVersionRepository#UNKNOWN_LOG_OFFSET} if starting from the middle of a file.
 */
public record LogChannel(
        long startAppendIndex,
        KernelVersion kernelVersion,
        StoreChannel channel,
        long startOffset,
        long endOffset,
        long lastAppendIndex,
        int previousChecksum)
        implements AutoCloseable {
    @Override
    public void close() throws Exception {
        channel.close();
    }
}
