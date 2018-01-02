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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class CountCommittedTransactionThresholdTest
{
    private final TriggerInfo triggerInfo = mock( TriggerInfo.class );
    @Test
    public void checkPointIsNotNeededWhenThereAreNoTransactions() throws Throwable
    {
        // given
        CountCommittedTransactionThreshold threshold = new CountCommittedTransactionThreshold( 2 );
        threshold.initialize( 2 );

        // when
        boolean checkPointingNeeded = threshold.isCheckPointingNeeded( 2, triggerInfo );

        // then
        assertFalse( checkPointingNeeded );
        verifyZeroInteractions( triggerInfo );
    }

    @Test
    public void checkPointIsNotNeededWhenTheNumberOfTransactionsIsUnderTheThreshold() throws Throwable
    {
        // given
        CountCommittedTransactionThreshold threshold = new CountCommittedTransactionThreshold( 2 );
        threshold.initialize( 2 );

        // when
        boolean checkPointingNeeded = threshold.isCheckPointingNeeded( 3, triggerInfo );

        // then
        assertFalse( checkPointingNeeded );
        verifyZeroInteractions( triggerInfo );
    }

    @Test
    public void checkPointIsNeededWhenTheNumberOfTransactionsIsEqualOrAboveTheThreshold() throws Throwable
    {
        // given
        CountCommittedTransactionThreshold threshold = new CountCommittedTransactionThreshold( 2 );
        threshold.initialize( 2 );

        // when
        boolean checkPointingNeeded = threshold.isCheckPointingNeeded( 4, triggerInfo );

        // then
        assertTrue( checkPointingNeeded );
        verify( triggerInfo, times( 1 ) ).accept( threshold.description() );
    }

    @Test
    public void checkPointIsNotNeededWhenThresholdIsReachedAndACheckPointHappened() throws Throwable
    {
        // given
        CountCommittedTransactionThreshold threshold = new CountCommittedTransactionThreshold( 2 );
        threshold.initialize( 2 );

        // when
        threshold.checkPointHappened( 4 );
        boolean checkPointingNeeded = threshold.isCheckPointingNeeded( 4, triggerInfo );

        // then
        assertFalse( checkPointingNeeded );
        verifyZeroInteractions( triggerInfo );
    }

    @Test
    public void checkPointIsNotNeededWhenThresholdIsReachedAndACheckPointHappenedAndSomeTransactionsHaveBeenCommitted()
            throws Throwable
    {
        // given
        CountCommittedTransactionThreshold threshold = new CountCommittedTransactionThreshold( 2 );
        threshold.initialize( 2 );

        // when
        threshold.checkPointHappened( 4 );
        boolean checkPointingNeeded = threshold.isCheckPointingNeeded( 5, triggerInfo );

        // then
        assertFalse( checkPointingNeeded );
        verifyZeroInteractions( triggerInfo );
    }

    @Test
    public void checkPointIsNeededWhenThresholdIsReachedAgainAfterACheckPointHappened() throws Throwable
    {
        // given
        CountCommittedTransactionThreshold threshold = new CountCommittedTransactionThreshold( 2 );
        threshold.initialize( 2 );

        // when
        threshold.checkPointHappened( 4 );
        boolean checkPointingNeeded = threshold.isCheckPointingNeeded( 6, triggerInfo );

        // then
        assertTrue( checkPointingNeeded );
        verify( triggerInfo, times( 1 ) ).accept( threshold.description() );
    }
}
