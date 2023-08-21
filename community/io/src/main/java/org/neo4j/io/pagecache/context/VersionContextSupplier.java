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
package org.neo4j.io.pagecache.context;

import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * Supplier to create {@link VersionContext} used during version data read and write operations
 */
public interface VersionContextSupplier {
    /**
     * Initialise current supplier with provider of transaction id snapshots
     * for future version context to be able to get snapshot of closed and visible transaction ids
     * @param transactionIdSnapshotFactory closed transaction id supplier.
     * @param oldestTransactionIdFactory
     */
    void init(
            TransactionIdSnapshotFactory transactionIdSnapshotFactory,
            OldestTransactionIdFactory oldestTransactionIdFactory);

    /**
     * Provide version context
     * @return instance of version context
     */
    VersionContext createVersionContext();

    @FunctionalInterface
    interface Factory {
        VersionContextSupplier create(NamedDatabaseId databaseId);
    }
}
