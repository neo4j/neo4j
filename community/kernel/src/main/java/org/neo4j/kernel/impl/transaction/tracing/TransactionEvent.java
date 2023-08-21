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
 * A trace event that represents a transaction with the database, and its lifetime.
 */
public interface TransactionEvent extends AutoCloseable {
    TransactionEvent NULL = new TransactionEvent() {
        @Override
        public void setCommit(boolean commit) {}

        @Override
        public void setRollback(boolean rollback) {}

        @Override
        public TransactionWriteEvent beginCommitEvent() {
            return TransactionWriteEvent.NULL;
        }

        @Override
        public void close() {}

        @Override
        public void setTransactionWriteState(String transactionWriteState) {}

        @Override
        public void setReadOnly(boolean wasReadOnly) {}

        @Override
        public TransactionWriteEvent beginChunkWriteEvent() {
            return TransactionWriteEvent.NULL;
        }

        @Override
        public TransactionRollbackEvent beginRollback() {
            return TransactionRollbackEvent.NULL;
        }
    };

    /**
     * The transaction was marked as successful.
     */
    void setCommit(boolean commit);

    /**
     * The transaction was marked as failed.
     */
    void setRollback(boolean rollback);

    /**
     * Begin the process of committing the transaction.
     */
    TransactionWriteEvent beginCommitEvent();

    /**
     * Begin the process of writing the transaction content to storage.
     */
    TransactionWriteEvent beginChunkWriteEvent();

    /**
     * Begin the process of transaction rollback.
     */
    TransactionRollbackEvent beginRollback();

    /**
     * Mark the end of the transaction, after it has been committed or rolled back.
     */
    @Override
    void close();

    /**
     * Set write state of the transaction
     */
    void setTransactionWriteState(String transactionWriteState);

    /**
     * Specify that the transaction was read-only.
     */
    void setReadOnly(boolean wasReadOnly);
}
