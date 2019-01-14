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

import org.junit.Test;

import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.reverse.EagerlyReversedTransactionCursor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.Iterators.array;
import static org.neo4j.kernel.impl.transaction.log.GivenTransactionCursor.exhaust;
import static org.neo4j.kernel.impl.transaction.log.GivenTransactionCursor.given;

public class EagerlyReversedTransactionCursorTest
{
    @Test
    public void shouldReverseTransactionsFromSource() throws Exception
    {
        // GIVEN
        CommittedTransactionRepresentation tx1 = mock( CommittedTransactionRepresentation.class );
        CommittedTransactionRepresentation tx2 = mock( CommittedTransactionRepresentation.class );
        CommittedTransactionRepresentation tx3 = mock( CommittedTransactionRepresentation.class );
        TransactionCursor source = given( tx1, tx2, tx3 );
        EagerlyReversedTransactionCursor cursor = new EagerlyReversedTransactionCursor( source );

        // WHEN
        CommittedTransactionRepresentation[] reversed = exhaust( cursor );

        // THEN
        assertArrayEquals( array( tx3, tx2, tx1 ), reversed );
    }

    @Test
    public void shouldHandleEmptySource() throws Exception
    {
        // GIVEN
        TransactionCursor source = given();
        EagerlyReversedTransactionCursor cursor = new EagerlyReversedTransactionCursor( source );

        // WHEN
        CommittedTransactionRepresentation[] reversed = exhaust( cursor );

        // THEN
        assertEquals( 0, reversed.length );
    }
}
