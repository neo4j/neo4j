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
package org.neo4j.storageengine.util;

import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.OpenTransactionMetadata;

public class ChunkedTransactionRegistry {
    private final NavigableMap<Long, ChunkEntry> chunkEntries = new ConcurrentSkipListMap<>();

    private record ChunkEntry(long appendIndex, LogPosition logPosition) {}

    public void registerTransaction(long transactionId, long appendIndex, LogPosition logPositionBefore) {
        chunkEntries.put(transactionId, new ChunkEntry(appendIndex, logPositionBefore));
    }

    public void removeTransaction(long transactionId) {
        chunkEntries.remove(transactionId);
    }

    public OpenTransactionMetadata oldestOpenTransactionMetadata() {
        Map.Entry<Long, ChunkEntry> firstEntry = chunkEntries.firstEntry();
        if (firstEntry == null) {
            return null;
        }
        ChunkEntry chunkEntry = firstEntry.getValue();
        return new OpenTransactionMetadata(firstEntry.getKey(), chunkEntry.appendIndex(), chunkEntry.logPosition);
    }
}
