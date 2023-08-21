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
package org.neo4j.internal.id;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.context.TransactionIdSnapshot;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.memory.MemoryTracker;

/**
 * Represent abstraction that responsible for any id related operations on a storage engine level: buffering,
 * maintenance, clearing, resetting, generation.
 */
public interface IdController extends Lifecycle {
    /**
     * Essentially a condition to check whether or not the {@link IdController} can free a batch of IDs, in maintenance.
     * For a concrete example it can be a snapshot of ongoing transactions. Then given that snapshot {@link #eligibleForFreeing(TransactionSnapshot)}
     * would check whether or not all of those transactions from the snapshot were closed.
     */
    interface IdFreeCondition {
        /**
         * @return whether or not the condition for freeing has been met so that maintenance can free a specific batch of ids.
         */
        boolean eligibleForFreeing(TransactionSnapshot snapshot);
    }

    record TransactionSnapshot(
            long currentSequenceNumber,
            long snapshotTimeMillis,
            long lastCommittedTransactionId,
            TransactionIdSnapshot idSnapshot) {
        public TransactionSnapshot(
                long currentSequenceNumber, long snapshotTimeMillis, long lastCommittedTransactionId) {
            this(
                    currentSequenceNumber,
                    snapshotTimeMillis,
                    lastCommittedTransactionId,
                    TransactionIdSnapshot.EMPTY_ID_SNAPSHOT);
        }
    }

    /**
     * Perform ids related maintenance.
     */
    void maintenance();

    void initialize(
            FileSystemAbstraction fs,
            Path baseBufferPath,
            Config config,
            Supplier<TransactionSnapshot> snapshotSupplier,
            IdFreeCondition condition,
            MemoryTracker memoryTracker,
            DatabaseReadOnlyChecker databaseReadOnlyChecker)
            throws IOException;
}
