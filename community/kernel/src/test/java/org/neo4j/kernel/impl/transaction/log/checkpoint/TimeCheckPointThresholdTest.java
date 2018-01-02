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

import org.neo4j.helpers.FakeClock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TimeCheckPointThresholdTest
{
    private final FakeClock clock = new FakeClock();
    private final TriggerInfo triggerInfo = mock( TriggerInfo.class );

    @Test
    public void shouldBeFalseIfTimeThresholdIsNotReachedAndThereAreCommittedTransactions() throws Throwable
    {
        // given
        TimeCheckPointThreshold threshold = new TimeCheckPointThreshold( 100, clock );
        threshold.initialize( 2 );

        clock.forward( 50, MILLISECONDS );

        // when
        boolean checkPointingNeeded = threshold.isCheckPointingNeeded( 42, triggerInfo );

        // then
        assertFalse( checkPointingNeeded );
        verifyZeroInteractions( triggerInfo );
    }

    @Test
    public void shouldBeTrueIfTimeThresholdIsReachedAndThereAreCommittedTransactions() throws Throwable
    {
        // given
        TimeCheckPointThreshold threshold = new TimeCheckPointThreshold( 100, clock );
        threshold.initialize( 2 );

        clock.forward( 100, MILLISECONDS );

        // when
        boolean checkPointingNeeded = threshold.isCheckPointingNeeded( 42, triggerInfo );

        // then
        assertTrue( checkPointingNeeded );
        verify( triggerInfo, times( 1 ) ).accept( threshold.description() );
    }

    @Test
    public void shouldBeFalseIfTimeThresholdIsReachedButThereAreNoCommittedTransactions() throws Throwable
    {
        // given
        TimeCheckPointThreshold threshold = new TimeCheckPointThreshold( 100, clock );
        threshold.initialize( 42 );

        clock.forward( 100, MILLISECONDS );

        // when
        boolean checkPointingNeeded = threshold.isCheckPointingNeeded( 42, triggerInfo );

        // then
        assertFalse( checkPointingNeeded );
        verifyZeroInteractions( triggerInfo );
    }

    @Test
    public void shouldBeFalseIfTimeThresholdIsReachedAfterCheckPointHappenedButThereAreNoCommittedTransactions()
            throws Throwable
    {
        // given
        TimeCheckPointThreshold threshold = new TimeCheckPointThreshold( 100, clock );
        threshold.initialize( 2 );

        clock.forward( 100, MILLISECONDS );

        threshold.checkPointHappened( 42 );

        clock.forward( 100, MILLISECONDS );

        // when
        boolean checkPointingNeeded = threshold.isCheckPointingNeeded( 42, triggerInfo );

        // then
        assertFalse( checkPointingNeeded );
        verifyZeroInteractions( triggerInfo );
    }

    @Test
    public void shouldBeTrueIfTimeThresholdIsReachedAfterCheckPointHappenedAndThereAreCommittedTransactions()
            throws Throwable
    {
        // given
        TimeCheckPointThreshold threshold = new TimeCheckPointThreshold( 100, clock );
        threshold.initialize( 2 );

        clock.forward( 100, MILLISECONDS );

        threshold.checkPointHappened( 42 );

        clock.forward( 100, MILLISECONDS );

        // when
        boolean checkPointingNeeded = threshold.isCheckPointingNeeded( 43, triggerInfo );

        // then
        assertTrue( checkPointingNeeded );
        verify( triggerInfo, times( 1 ) ).accept( threshold.description() );
    }
}
