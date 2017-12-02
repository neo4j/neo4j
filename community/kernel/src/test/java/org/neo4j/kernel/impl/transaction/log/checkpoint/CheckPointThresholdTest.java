/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CheckPointThresholdTest extends CheckPointThresholdTestSupport
{
    @Test
    public void mustCreateThresholdThatTriggersAfterTransactionCount() throws Exception
    {
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 1 ); // Initialise at transaction id offset by 1.

        // False because we're not yet at threshold.
        assertFalse( threshold.isCheckPointingNeeded( intervalTx - 1, notTriggered ) );
        // Still false because the counter is offset by one, since we initialised with 1.
        assertFalse( threshold.isCheckPointingNeeded( intervalTx, notTriggered ) );
        // True because new we're at intervalTx + initial offset.
        assertTrue( threshold.isCheckPointingNeeded( intervalTx + 1, triggered ) );
        verifyTriggered( "count" );
        verifyNoMoreTriggers();
    }

    @Test
    public void mustCreateThresholdThatTriggersAfterTime() throws Exception
    {
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 1 );
        // Skip the initial wait period.
        clock.forward( intervalTime.toMillis(), MILLISECONDS );
        // The clock will trigger at a random point within the interval in the future.

        // False because we haven't moved the clock, or the transaction count.
        assertFalse( threshold.isCheckPointingNeeded( 2, notTriggered ) );
        // True because we now moved forward by an interval.
        clock.forward( intervalTime.toMillis(), MILLISECONDS );
        assertTrue( threshold.isCheckPointingNeeded( 4, triggered ) );
        verifyTriggered( "time" );
        verifyNoMoreTriggers();
    }

    @Test
    public void mustNotTriggerBeforeTimeWithTooFewCommittedTransactions() throws Throwable
    {
        withIntervalTime( "100ms" );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        clock.forward( 50, MILLISECONDS );
        assertFalse( threshold.isCheckPointingNeeded( 42, notTriggered ) );
    }

    @Test
    public void mustTriggerWhenTimeThresholdIsReachedAndThereAreCommittedTransactions() throws Throwable
    {
        withIntervalTime( "100ms" );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        clock.forward( 199, MILLISECONDS );

        assertTrue( threshold.isCheckPointingNeeded( 42, triggered ) );
        verifyTriggered( "time" );
        verifyNoMoreTriggers();
    }

    @Test
    public void mustNotTriggerWhenTimeThresholdIsReachedAndThereAreNoCommittedTransactions() throws Throwable
    {
        withIntervalTime( "100ms" );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 42 );

        clock.forward( 199, MILLISECONDS );

        assertFalse( threshold.isCheckPointingNeeded( 42, notTriggered ) );
        verifyNoMoreTriggers();
    }

    @Test
    public void mustNotTriggerPastTimeThresholdSinceLastCheckpointWithNoNewTransactions()
    {
        withIntervalTime( "100ms" );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        clock.forward( 199, MILLISECONDS );
        threshold.checkPointHappened( 42 );
        clock.forward( 100, MILLISECONDS );

        assertFalse( threshold.isCheckPointingNeeded( 42, notTriggered ) );
        verifyNoMoreTriggers();
    }

    @Test
    public void mustTriggerPastTimeThresholdSinceLastCheckpointWithNewTransactions()
    {
        withIntervalTime( "100ms" );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        clock.forward( 199, MILLISECONDS );
        threshold.checkPointHappened( 42 );
        clock.forward( 100, MILLISECONDS );

        assertTrue( threshold.isCheckPointingNeeded( 43, triggered ) );
        verifyTriggered( "time" );
        verifyNoMoreTriggers();
    }

    @Test
    public void mustNotTriggerOnTransactionCountWhenThereAreNoNewTransactions() throws Throwable
    {
        withIntervalTx( 2 );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        assertFalse( threshold.isCheckPointingNeeded( 2, notTriggered ) );
    }

    @Test
    public void mustNotTriggerOnTransactionCountWhenCountIsBellowThreshold() throws Throwable
    {
        withIntervalTx( 2 );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        assertFalse( threshold.isCheckPointingNeeded( 3, notTriggered ) );
    }

    @Test
    public void mustTriggerOnTransactionCountWhenCountIsAtThreshold() throws Throwable
    {
        withIntervalTx( 2 );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        assertTrue( threshold.isCheckPointingNeeded( 4, triggered ) );
        verifyTriggered( "count" );
        verifyNoMoreTriggers();
    }

    @Test
    public void mustNotTriggerOnTransactionCountAtThresholdIfCheckPointAlreadyHappened() throws Throwable
    {
        withIntervalTx( 2 );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        threshold.checkPointHappened( 4 );
        assertFalse( threshold.isCheckPointingNeeded( 4, notTriggered ) );
    }

    @Test
    public void mustNotTriggerWhenTransactionCountIsWithinThresholdSinceLastTrigger() throws Exception
    {
        withIntervalTx( 2 );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        threshold.checkPointHappened( 4 );
        assertFalse( threshold.isCheckPointingNeeded( 5, notTriggered ) );
    }

    @Test
    public void mustTriggerOnTransactionCountWhenCountIsAtThresholdSinceLastCheckPoint() throws Throwable
    {
        withIntervalTx( 2 );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        threshold.checkPointHappened( 4 );
        assertTrue( threshold.isCheckPointingNeeded( 6, triggered ) );
        verifyTriggered( "count" );
        verifyNoMoreTriggers();
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    public void timeBasedThresholdMustSuggestSchedulingFrequency() throws Exception
    {
        long defaultInterval = intervalTime.toMillis();
        assertThat( createThreshold().checkFrequencyMillis().min().getAsLong(), is( defaultInterval ) );

        withIntervalTime( "100ms" );
        assertThat( createThreshold().checkFrequencyMillis().min().getAsLong(), is( 100L ) );
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    public void continuousPolicyMustAlwaysTriggerCheckPoints() throws Exception
    {
        withPolicy( "continuously" );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        assertThat( threshold.checkFrequencyMillis().min().getAsLong(), is( 0L ) );

        assertTrue( threshold.isCheckPointingNeeded( 2, triggered ) );
        threshold.checkPointHappened( 3 );
        assertTrue( threshold.isCheckPointingNeeded( 3, triggered ) );
        assertTrue( threshold.isCheckPointingNeeded( 3, triggered ) );
        verifyTriggered( "continuous" );
        verifyTriggered( "continuous" );
        verifyTriggered( "continuous" );
        verifyNoMoreTriggers();
    }
}
