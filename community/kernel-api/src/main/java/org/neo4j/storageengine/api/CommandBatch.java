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
package org.neo4j.storageengine.api;

import org.neo4j.common.Subject;
import org.neo4j.kernel.KernelVersionProvider;

/**
 * Representation of a transaction that can be written to transaction log and read back later.
 */
public interface CommandBatch extends CommandStream, KernelVersionProvider {
    /**
     * Command batch consensus index
     */
    long consensusIndex();

    void setConsensusIndex(long commandIndex);

    /**
     * @return time when transaction was started, i.e. when the user started it, not when it was committed.
     * Reported in milliseconds.
     */
    long getTimeStarted();

    /**
     * @return last committed transaction id at the time when this transaction was started.
     */
    long getLatestCommittedTxWhenStarted();

    /**
     * @return time when transaction was committed. Reported in milliseconds.
     */
    long getTimeCommitted();

    /**
     * @return the identifier for the lease associated with this transaction.
     * This is only used for coordinating transaction validity in a cluster.
     */
    int getLeaseId();

    /**
     * @return the subject associated with the transaction.
     * Typically, an authenticated end user that created the transaction.
     */
    Subject subject();

    /**
     * A to-string method that may include information about all the commands in this transaction.
     * @param includeCommands whether to include commands in the returned string.
     * @return information about this transaction representation w/ or w/o command information included.
     */
    String toString(boolean includeCommands);

    /**
     * True if command batch is the last batch in the sequence of transactional command batches.
     */
    boolean isLast();

    /**
     * True if command batch is the first batch in the sequence of transactional command batches.
     */
    boolean isFirst();

    /**
     * True if command batch is a rollback batch for one of the transactions.
     */
    boolean isRollback();

    /**
     * Number of commands in a batch
     */
    int commandCount();

    /**
     * Get command batch append index
     */
    long appendIndex();

    /**
     * Set command batch append index. Append index of command batch becomes available only after appending it into the log files.
     * @param appendIndex provided batch append index
     */
    void setAppendIndex(long appendIndex);
}
