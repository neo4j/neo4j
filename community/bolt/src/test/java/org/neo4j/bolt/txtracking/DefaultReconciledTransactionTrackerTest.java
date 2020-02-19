/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.txtracking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.test.extension.SuppressOutputExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class DefaultReconciledTransactionTrackerTest
{
    private DefaultReconciledTransactionTracker tracker;

    @BeforeEach
    void beforeEach()
    {
        tracker = new DefaultReconciledTransactionTracker( new SimpleLogService( FormattedLogProvider.toOutputStream( System.out ) ) );
    }

    @Test
    void shouldReturnDummyReconciledTransactionIdWhenNotInitialized()
    {
        assertEquals( -1, tracker.getLastReconciledTransactionId() );
    }

    @Test
    void shouldReturnReconciledTransactionIdWhenInitializedButNeverUpdated()
    {
        tracker.enable( 42 );

        assertEquals( 42, tracker.getLastReconciledTransactionId() );
    }

    @Test
    void shouldReturnReconciledTransactionIdWhenReInitialized()
    {
        tracker.enable( 42 );
        tracker.enable( 4242 );
        tracker.enable( 424242 );

        assertEquals( 424242, tracker.getLastReconciledTransactionId() );
    }

    @Test
    void shouldReturnReconciledTransactionIdWhenInitializedAndUpdated()
    {
        tracker.enable( 1 );

        tracker.offerReconciledTransactionId( 7 );
        tracker.offerReconciledTransactionId( 2 );
        tracker.offerReconciledTransactionId( 3 );
        tracker.offerReconciledTransactionId( 5 );
        tracker.offerReconciledTransactionId( 4 );

        assertEquals( 5, tracker.getLastReconciledTransactionId() );
    }

    @Test
    void shouldFailToInitializeWithNegativeTransactionId()
    {
        assertThrows( IllegalArgumentException.class, () -> tracker.enable( -42 ) );
    }

    @Test
    void shouldFailToUpdateWithNegativeTransactionId()
    {
        tracker.enable( 42 );

        assertThrows( IllegalArgumentException.class, () -> tracker.offerReconciledTransactionId( -42 ) );
    }

    @Test
    void shouldApplyQueueWhenFirstEnabled()
    {
        tracker.offerReconciledTransactionId( 42 );
        tracker.offerReconciledTransactionId( 43 );
        tracker.offerReconciledTransactionId( 44 );
        tracker.offerReconciledTransactionId( 45 );
        assertEquals( -1, tracker.getLastReconciledTransactionId() );

        tracker.enable( 41 );
        assertEquals( 45, tracker.getLastReconciledTransactionId() );
    }

    @Test
    void shouldKeepLastValueWhenDisabledAndApplyQueueWhenEnabled()
    {
        tracker.enable( 42 );

        tracker.offerReconciledTransactionId( 43 );
        tracker.offerReconciledTransactionId( 44 );

        tracker.disable();
        assertEquals( 44, tracker.getLastReconciledTransactionId() );

        tracker.offerReconciledTransactionId( 52 );
        tracker.offerReconciledTransactionId( 51 );
        tracker.offerReconciledTransactionId( 50 );
        tracker.offerReconciledTransactionId( 49 );
        tracker.offerReconciledTransactionId( 48 );
        assertEquals( 44, tracker.getLastReconciledTransactionId() );

        tracker.enable( 49 );
        assertEquals( 52, tracker.getLastReconciledTransactionId() );
    }

    @Test
    void shouldFailToUpdateWithNonIncreasingTransactionId()
    {
        tracker.enable( 1 );

        tracker.offerReconciledTransactionId( 2 );
        tracker.offerReconciledTransactionId( 3 );
        tracker.offerReconciledTransactionId( 4 );

        assertThrows( IllegalArgumentException.class, () -> tracker.offerReconciledTransactionId( 2 ) );
    }

    @Test
    void shouldIgnorePreInitializationIds()
    {
        tracker.enable( 1 );
        tracker.offerReconciledTransactionId( 0 );
        assertEquals( 1, tracker.getLastReconciledTransactionId() );

        tracker.enable( 42 );
        tracker.offerReconciledTransactionId( 40 );
        tracker.offerReconciledTransactionId( 41 );
        assertEquals( 42, tracker.getLastReconciledTransactionId() );

        tracker.offerReconciledTransactionId( 43 );
        assertEquals( 43, tracker.getLastReconciledTransactionId() );
    }
}
