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

import org.neo4j.logging.Log;

import static java.lang.String.format;

public class ReverseTransactionCursorLoggingMonitor implements ReversedTransactionCursorMonitor
{
    private final Log log;

    public ReverseTransactionCursorLoggingMonitor( Log log )
    {
        this.log = log;
    }

    @Override
    public void transactionalLogRecordReadFailure( long[] transactionOffsets, int transactionIndex, long logVersion )
    {
        log.warn( transactionIndex > 0 ?
               format( "Fail to read transaction log version %d. Last valid transaction start offset is: %d.",
                       logVersion, transactionOffsets[transactionIndex - 1] ) :
               format( "Fail to read first transaction of log version %d.", logVersion) );
    }
}
