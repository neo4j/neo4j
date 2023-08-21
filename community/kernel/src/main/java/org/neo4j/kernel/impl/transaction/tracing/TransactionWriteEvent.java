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
package org.neo4j.kernel.impl.transaction.tracing;

/**
 * A trace event that represents the commit process of a transaction.
 */
public interface TransactionWriteEvent extends AutoCloseable {
    TransactionWriteEvent NULL = new TransactionWriteEvent() {
        @Override
        public void close() {}

        @Override
        public LogAppendEvent beginLogAppend() {
            return LogAppendEvent.NULL;
        }

        @Override
        public StoreApplyEvent beginStoreApply() {
            return StoreApplyEvent.NULL;
        }

        @Override
        public void chunkAppended(int chunkNumber, long transactionSequenceNumber, long transactionId) {}
    };

    /**
     * Mark the end of the commit process.
     */
    @Override
    void close();

    /**
     * Begin appending commands for the committing transaction, to the transaction log.
     */
    LogAppendEvent beginLogAppend();

    /**
     * Begin applying the commands of the committed transaction to the stores.
     */
    StoreApplyEvent beginStoreApply();

    /**
     * Chunk appended notification
     * @param chunkNumber number of appended chunk in transaction
     * @param transactionSequenceNumber sequence number of transaction
     * @param transactionId transaction id
     */
    void chunkAppended(int chunkNumber, long transactionSequenceNumber, long transactionId);
}
