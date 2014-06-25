/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;


/**
 * This class represents the concept of a TransactionRepresentation that has been
 * committed to the TransactionStore. It contains, in addition to the TransactionRepresentation
 * itself, a Start and Commit entry. This is the thing that {@link LogicalTransactionStore} returns when
 * asked for a transaction via a cursor.
 */
// TODO 2.2-future make this visitable in the same way TransactionRepresentation is, if necessary
// TODO The way it is setup now, it means that the TransactionRepresentation has to be wholly in memory before
// TODO acting on it, which precludes the ability to stream the thing. This may not be good short term.
public class CommittedTransactionRepresentation
{
    private final LogEntry.Start startEntry;
    private final TransactionRepresentation transactionRepresentation;
    private final LogEntry.Commit commitEntry;

    public CommittedTransactionRepresentation( LogEntry.Start startEntry, TransactionRepresentation
            transactionRepresentation, LogEntry.Commit commitEntry )
    {
        this.startEntry = startEntry;
        this.transactionRepresentation = transactionRepresentation;
        this.commitEntry = commitEntry;
    }

    public LogEntry.Start getStartEntry()
    {
        return startEntry;
    }

    public TransactionRepresentation getTransactionRepresentation()
    {
        return transactionRepresentation;
    }

    public LogEntry.Commit getCommitEntry()
    {
        return commitEntry;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() +
                "[" + startEntry + ", " + transactionRepresentation + ", " + commitEntry + "]";
    }
}
