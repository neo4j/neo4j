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
package org.neo4j.kernel.impl.transaction.log;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

/**
 * Log index is encoded in the header of transactions in the transaction log.
 */
public class LogIndexEncoding {
    private LogIndexEncoding() {}

    public static byte[] encodeLogIndex(long logIndex) {
        if (logIndex == UNKNOWN_CONSENSUS_INDEX) {
            return EMPTY_BYTE_ARRAY;
        }

        byte[] b = new byte[Long.BYTES];
        for (int i = Long.BYTES - 1; i > 0; i--) {
            b[i] = (byte) logIndex;
            logIndex >>>= Byte.SIZE;
        }
        b[0] = (byte) logIndex;
        return b;
    }

    public static long decodeLogIndex(byte[] bytes) {
        if (bytes.length == 0) {
            return UNKNOWN_CONSENSUS_INDEX;
        }
        if (bytes.length < Long.BYTES) {
            throw new IllegalArgumentException("Unable to decode log index from the transaction header.");
        }

        long logIndex = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            logIndex <<= Byte.SIZE;
            logIndex ^= bytes[i] & 0xFF;
        }
        return logIndex;
    }
}
