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
package org.neo4j.kernel.impl.transaction.log.reverse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;

/**
 * Eagerly exhausts a {@link TransactionCursor} and allows moving through it in reverse order.
 * The idea is that this should only be done for a subset of a bigger transaction log stream, typically
 * for one log file.
 *
 * For reversing a transaction log consisting of multiple log files {@link ReversedMultiFileTransactionCursor}
 * should be used (it will use this class internally though).
 *
 * @see ReversedMultiFileTransactionCursor
 */
public class EagerlyReversedTransactionCursor implements TransactionCursor
{
    private final List<CommittedTransactionRepresentation> txs = new ArrayList<>();
    private final TransactionCursor cursor;
    private int indexToReturn;

    public EagerlyReversedTransactionCursor( TransactionCursor cursor ) throws IOException
    {
        this.cursor = cursor;
        while ( cursor.next() )
        {
            txs.add( cursor.get() );
        }
        this.indexToReturn = txs.size();
    }

    @Override
    public boolean next()
    {
        if ( indexToReturn > 0 )
        {
            indexToReturn--;
            return true;
        }
        return false;
    }

    @Override
    public void close() throws IOException
    {
        cursor.close();
    }

    @Override
    public CommittedTransactionRepresentation get()
    {
        return txs.get( indexToReturn );
    }

    @Override
    public LogPosition position()
    {
        throw new UnsupportedOperationException( "Should not be called" );
    }

    public static TransactionCursor eagerlyReverse( TransactionCursor cursor ) throws IOException
    {
        return new EagerlyReversedTransactionCursor( cursor );
    }
}
