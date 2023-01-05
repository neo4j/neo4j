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
package org.neo4j.kernel.impl.transaction.log.entry.v54;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.CHUNK_END;

import org.neo4j.kernel.impl.transaction.log.entry.AbstractLogEntry;

public class LogEntryChunkEnd extends AbstractLogEntry {
    private final long transactionId;
    private final long chunkId;
    private final int checksum;

    public LogEntryChunkEnd(long transactionId, long chunkId, int checksum) {
        super(CHUNK_END);
        this.transactionId = transactionId;
        this.chunkId = chunkId;
        this.checksum = checksum;
    }

    public long getChunkId() {
        return chunkId;
    }

    public int getChecksum() {
        return checksum;
    }

    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public String toString() {
        return "LogEntryChunkEnd{" + "chunkId=" + chunkId + ", checksum=" + checksum + ", txId=" + transactionId + '}';
    }
}
