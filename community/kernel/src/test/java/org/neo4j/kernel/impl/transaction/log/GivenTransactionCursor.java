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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;

public class GivenTransactionCursor implements TransactionCursor
{
    private int index = -1;
    private final CommittedTransactionRepresentation[] transactions;

    private GivenTransactionCursor( CommittedTransactionRepresentation... transactions )
    {
        this.transactions = transactions;
    }

    @Override
    public CommittedTransactionRepresentation get()
    {
        return transactions[index];
    }

    @Override
    public boolean next()
    {
        if ( index + 1 < transactions.length )
        {
            index++;
            return true;
        }
        return false;
    }

    @Override
    public void close()
    {
    }

    @Override
    public LogPosition position()
    {
        return null;
    }

    public static TransactionCursor given( CommittedTransactionRepresentation... transactions )
    {
        return new GivenTransactionCursor( transactions );
    }

    public static CommittedTransactionRepresentation[] exhaust( TransactionCursor cursor ) throws IOException
    {
        List<CommittedTransactionRepresentation> list = new ArrayList<>();
        while ( cursor.next() )
        {
            list.add( cursor.get() );
        }
        return list.toArray( new CommittedTransactionRepresentation[list.size()] );
    }
}
