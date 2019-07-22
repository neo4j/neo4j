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
        tracker.initialize( 42 );

        assertEquals( 42, tracker.getLastReconciledTransactionId() );
    }

    @Test
    void shouldReturnReconciledTransactionIdWhenReInitialized()
    {
        tracker.initialize( 42 );
        tracker.initialize( 4242 );
        tracker.initialize( 424242 );

        assertEquals( 424242, tracker.getLastReconciledTransactionId() );
    }

    @Test
    void shouldReturnReconciledTransactionIdWhenInitializedAndUpdated()
    {
        tracker.initialize( 1 );

        tracker.setLastReconciledTransactionId( 7 );
        tracker.setLastReconciledTransactionId( 2 );
        tracker.setLastReconciledTransactionId( 3 );
        tracker.setLastReconciledTransactionId( 5 );
        tracker.setLastReconciledTransactionId( 4 );

        assertEquals( 5, tracker.getLastReconciledTransactionId() );
    }

    @Test
    void shouldFailToInitializeWithNegativeTransactionId()
    {
        assertThrows( IllegalArgumentException.class, () -> tracker.initialize( -42 ) );
    }

    @Test
    void shouldFailToUpdateWithNegativeTransactionId()
    {
        tracker.initialize( 42 );

        assertThrows( IllegalArgumentException.class, () -> tracker.setLastReconciledTransactionId( -42 ) );
    }

    @Test
    void shouldFailToUpdateWhenNotInitialized()
    {
        assertThrows( IllegalStateException.class, () -> tracker.setLastReconciledTransactionId( 42 ) );
    }

    @Test
    void shouldFailToUpdateWithNonIncreasingTransactionId()
    {
        tracker.initialize( 1 );

        tracker.setLastReconciledTransactionId( 2 );
        tracker.setLastReconciledTransactionId( 3 );
        tracker.setLastReconciledTransactionId( 4 );

        assertThrows( IllegalArgumentException.class, () -> tracker.setLastReconciledTransactionId( 2 ) );
    }
}
