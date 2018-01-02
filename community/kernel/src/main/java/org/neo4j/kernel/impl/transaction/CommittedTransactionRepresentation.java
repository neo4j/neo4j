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
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;

/**
 * This class represents the concept of a TransactionRepresentation that has been
 * committed to the TransactionStore. It contains, in addition to the TransactionRepresentation
 * itself, a Start and Commit entry. This is the thing that {@link LogicalTransactionStore} returns when
 * asked for a transaction via a cursor.
 */
public class CommittedTransactionRepresentation
{
    private final LogEntryStart startEntry;
    private final TransactionRepresentation transactionRepresentation;
    private final LogEntryCommit commitEntry;

    public CommittedTransactionRepresentation( LogEntryStart startEntry, TransactionRepresentation
            transactionRepresentation, LogEntryCommit commitEntry )
    {
        this.startEntry = startEntry;
        this.transactionRepresentation = transactionRepresentation;
        this.commitEntry = commitEntry;
    }

    public void accept( Visitor<Command, IOException> visitor ) throws IOException
    {
        transactionRepresentation.accept( visitor );
    }

    public LogEntryStart getStartEntry()
    {
        return startEntry;
    }

    public TransactionRepresentation getTransactionRepresentation()
    {
        return transactionRepresentation;
    }

    public LogEntryCommit getCommitEntry()
    {
        return commitEntry;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() +
                "[" + startEntry + ", " + transactionRepresentation + ", " + commitEntry + "]";
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        CommittedTransactionRepresentation that = (CommittedTransactionRepresentation) o;

        if ( !commitEntry.equals( that.commitEntry ) )
        {
            return false;
        }
        if ( !startEntry.equals( that.startEntry ) )
        {
            return false;
        }
        if ( !transactionRepresentation.equals( that.transactionRepresentation ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = startEntry.hashCode();
        result = 31 * result + transactionRepresentation.hashCode();
        result = 31 * result + commitEntry.hashCode();
        return result;
    }
}
