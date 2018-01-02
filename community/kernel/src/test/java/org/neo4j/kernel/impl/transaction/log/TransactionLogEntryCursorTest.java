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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.COMMAND;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_1P_COMMIT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_START;

public class TransactionLogEntryCursorTest
{
    @Test
    public void shouldDeliverIntactTransactions() throws IOException
    {
        // GIVEN
        // tx 1
        List<LogEntry> tx1 = makeTransaction( TX_START, COMMAND, TX_1P_COMMIT );

        // tx 2
        List<LogEntry> tx2 = makeTransaction( TX_START, COMMAND, COMMAND, TX_1P_COMMIT );

        // All transactions

        // The cursor
        TransactionLogEntryCursor transactionCursor = new TransactionLogEntryCursor( new ArrayIOCursor(
                transactionsAsArray( tx1, tx2 ) ) );

        // THEN
        // tx1
        assertTrue( transactionCursor.next() );
        assertTx( tx1, transactionCursor.get() );

        // tx2
        assertTrue( transactionCursor.next() );
        assertTx( tx2, transactionCursor.get() );

        // No more transactions
        assertFalse( transactionCursor.next() );
    }

    @Test
    public void shouldNotDeliverTransactionsWithoutEnd() throws IOException
    {
        // GIVEN
        // tx 1
        List<LogEntry> tx1 = makeTransaction( TX_START, COMMAND, COMMAND, COMMAND, TX_1P_COMMIT );

        // tx 2
        List<LogEntry> tx2 = makeTransaction( TX_START, COMMAND, COMMAND );

        TransactionLogEntryCursor transactionCursor = new TransactionLogEntryCursor( new ArrayIOCursor(
                transactionsAsArray( tx1, tx2 ) ) );

        // THEN
        assertTrue( transactionCursor.next() );
        assertTx( tx1, transactionCursor.get() );

        assertFalse( transactionCursor.next() );
    }

    private LogEntry[] transactionsAsArray( List<LogEntry>... transactions )
    {
        List<LogEntry> allTransactions = new ArrayList<>();
        for ( List<LogEntry> tx : transactions )
        {
            allTransactions.addAll( tx );
        }
        return allTransactions.toArray( new LogEntry[allTransactions.size()] );
    }

    private void assertTx( List<LogEntry> expected, LogEntry[] actual )
    {
        assertArrayEquals( expected.toArray( new LogEntry[expected.size()] ), actual );
    }

    private List<LogEntry> makeTransaction( byte... types )
    {
        List<LogEntry> transaction = new ArrayList<>( types.length );
        for ( Byte type : types )
        {
            transaction.add( mockedLogEntry( type ) );
        }
        return transaction;
    }

    private LogEntry mockedLogEntry( byte type )
    {
        LogEntry logEntry = mock( LogEntry.class );
        when( logEntry.getType() ).thenReturn( type );
        return logEntry;
    }
}