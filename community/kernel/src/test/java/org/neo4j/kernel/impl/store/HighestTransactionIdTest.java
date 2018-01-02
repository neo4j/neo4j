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
package org.neo4j.kernel.impl.store;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.test.Race;

import static java.lang.Math.max;
import static java.lang.Runtime.getRuntime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HighestTransactionIdTest
{
    @Test
    public void shouldHardSetHighest() throws Exception
    {
        // GIVEN
        HighestTransactionId highest = new HighestTransactionId( 10, 10, 10 );

        // WHEN
        highest.set( 8, 1299128, 42 );

        // THEN
        assertEquals( new TransactionId( 8, 1299128, 42 ), highest.get() );
    }

    @Test
    public void shouldOnlyKeepTheHighestOffered() throws Exception
    {
        // GIVEN
        HighestTransactionId highest = new HighestTransactionId( -1, -1, -1 );

        // WHEN/THEN
        assertAccepted( highest, 2 );
        assertAccepted( highest, 5 );
        assertRejected( highest, 3 );
        assertRejected( highest, 4 );
        assertAccepted( highest, 10 );
    }

    @Test
    public void shouldKeepHighestDuringConcurrentOfferings() throws Throwable
    {
        // GIVEN
        final HighestTransactionId highest = new HighestTransactionId( -1, -1, -1 );
        Race race = new Race();
        int updaters = max( 2, getRuntime().availableProcessors() );
        final AtomicInteger accepted = new AtomicInteger();
        for ( int i = 0; i < updaters; i++ )
        {
            final long id = i + 1;
            race.addContestant( new Runnable()
            {
                @Override
                public void run()
                {
                    if ( highest.offer( id, id, id ) )
                    {
                        accepted.incrementAndGet();
                    }
                }
            } );
        }

        // WHEN
        race.go();

        // THEN
        assertTrue( accepted.get() > 0 );
        assertEquals( updaters, highest.get().transactionId() );
    }

    private void assertAccepted( HighestTransactionId highest, long txId )
    {
        TransactionId current = highest.get();
        assertTrue( highest.offer( txId, -1, -1 ) );
        assertTrue( txId > current.transactionId() );
    }

    private void assertRejected( HighestTransactionId highest, long txId )
    {
        TransactionId current = highest.get();
        assertFalse( highest.offer( txId, -1, -1 ) );
        assertEquals( current, highest.get() );
    }
}
