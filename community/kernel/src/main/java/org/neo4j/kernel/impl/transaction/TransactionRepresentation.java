/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.storageengine.api.CommandStream;

/**
 * Representation of a transaction that can be written to a {@link TransactionAppender} and read back later.
 */
public interface TransactionRepresentation extends CommandStream
{
    /**
     * @return an additional header of this transaction. Just arbitrary bytes that means nothing
     * to this transaction representation.
     */
    byte[] additionalHeader();

    /**
     * @return database instance id of current master in a potential database cluster at the time of committing
     * this transaction {@code -1} means no cluster.
     */
    int getMasterId();

    /**
     * @return database instance id of the author of this transaction.
     */
    int getAuthorId();

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
     * @return the identifier for the lock session associated with this transaction, or {@value Locks.Client#NO_LOCK_SESSION_ID} if none.
     * This is only used for slave commits.
     */
    int getLockSessionId();
}
