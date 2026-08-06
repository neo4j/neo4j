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
package org.neo4j.kernel.impl.transaction.log.distributed;

public enum BatchType {
    COMPLETE((byte) 0),
    CHUNKED((byte) 1),
    STORAGE_ENGINE_ID_ONLY_HEADER((byte) 2),
    STORAGE_ENGINE_ID_ONLY_HEADER_CHUNKED((byte) 3),
    STANDALONE_TX_NO_HEADER((byte) 4);

    private final byte byteValue;

    BatchType(byte byteValue) {
        this.byteValue = byteValue;
    }

    public byte byteValue() {
        return byteValue;
    }

    public static BatchType ofByte(byte value) {
        return switch (value) {
            case 0 -> COMPLETE;
            case 1 -> CHUNKED;
            case 2 -> STORAGE_ENGINE_ID_ONLY_HEADER;
            case 3 -> STORAGE_ENGINE_ID_ONLY_HEADER_CHUNKED;
            case 4 -> STANDALONE_TX_NO_HEADER;
            default -> throw new IllegalStateException("Unexpected value: " + value);
        };
    }

    /**
     * Check if the TransactionHeader is only partial
     * @return true if the TransactionHeader is minimal (storage id and chunked/complete),
     * or missing (standalone). Information has to be taken from kernel LogEntries only
     * If this returns false all header values are present and may be used if for instance the
     * transaction has not yet had finalized values assigned.
     */
    public boolean hasMiniHeader() {
        return this == STORAGE_ENGINE_ID_ONLY_HEADER
                || this == STORAGE_ENGINE_ID_ONLY_HEADER_CHUNKED
                || this == STANDALONE_TX_NO_HEADER;
    }

    /**
     * For single chunk MVCC transactions the kernel LogEntries can be ambiguous with non-MVCC transactions,
     * so the TransactionHeader helps disambiguate them.
     * For standalone transactions it is still ambiguous, but we only return false as
     * this should only be encountered on entries before the start of the RaftGroup
     * and they will not require replicated application
     * @return true if the header records the transaction as MVCC, else false
     */
    public boolean isChunked() {
        return this == CHUNKED || this == STORAGE_ENGINE_ID_ONLY_HEADER_CHUNKED;
    }
}
