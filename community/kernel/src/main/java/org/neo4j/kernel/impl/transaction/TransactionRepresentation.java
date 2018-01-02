/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;

/**
 * Representation of a transaction that can be written to a {@link TransactionAppender} and read back later.
 */
public interface TransactionRepresentation
{
    /**
     * Accepts a visitor into the commands making up this transaction.
     * @param visitor {@link Visitor} which will see the commands.
     * @throws IOException if there were any problem reading the commands.
     */
    void accept( Visitor<Command, IOException> visitor ) throws IOException;

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
     * @return the identifier for the lock session associated with this transaction, or {@link #NO_LOCK_SESSION} if none. This is only used for slave commits.
     */
    int getLockSessionId();
}
