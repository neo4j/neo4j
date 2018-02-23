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

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class LogVersionLocatorTest
{
    private final long firstTxIdInLog = 3;
    private final long lastTxIdInLog = 67;

    @Test
    public void shouldFindLogPosition() throws NoSuchTransactionException
    {
        // given
        final long txId = 42L;

        final PhysicalLogicalTransactionStore.LogVersionLocator locator =
                new PhysicalLogicalTransactionStore.LogVersionLocator( txId );

        final LogPosition position = new LogPosition( 1, 128 );

        // when
        final boolean result = locator.visit( position, firstTxIdInLog, lastTxIdInLog );

        // then
        assertFalse( result );
        assertEquals( position, locator.getLogPosition() );
    }

    @Test
    public void shouldNotFindLogPosition()
    {
        // given
        final long txId = 1L;

        final PhysicalLogicalTransactionStore.LogVersionLocator locator =
                new PhysicalLogicalTransactionStore.LogVersionLocator( txId );

        final LogPosition position = new LogPosition( 1, 128 );

        // when
        final boolean result = locator.visit( position, firstTxIdInLog, lastTxIdInLog );

        // then
        assertTrue( result );

        try
        {
            locator.getLogPosition();
            fail( "should have thrown" );
        }
        catch ( NoSuchTransactionException e )
        {
            assertEquals(
                    "Unable to find transaction " + txId + " in any of my logical logs: " +
                            "Couldn't find any log containing " + txId,
                    e.getMessage()
            );
        }
    }

    @Test
    public void shouldAlwaysThrowIfVisitIsNotCalled()
    {
        // given
        final long txId = 1L;

        final PhysicalLogicalTransactionStore.LogVersionLocator locator =
                new PhysicalLogicalTransactionStore.LogVersionLocator( txId );

        // then
        try
        {
            locator.getLogPosition();
            fail( "should have thrown" );
        }
        catch ( NoSuchTransactionException e )
        {
            assertEquals(
                    "Unable to find transaction " + txId + " in any of my logical logs: " +
                            "Couldn't find any log containing " + txId,
                    e.getMessage()
            );
        }
    }
}
