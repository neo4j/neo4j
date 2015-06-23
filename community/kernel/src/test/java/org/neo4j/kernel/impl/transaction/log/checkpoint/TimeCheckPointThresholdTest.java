/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

public class TimeCheckPointThresholdTest
{
    private FakeClock clock = new FakeClock();

    @Test
    public void shouldBeFalseIfTimeThresholdIsNotReachedAndThereAreCommittedTransactions() throws Throwable
    {
        // given
        TimeCheckPointThreshold timeCheckPointThreshold = new TimeCheckPointThreshold( 100, clock );
        timeCheckPointThreshold.initialize( 2 );

        clock.forward( 50, MILLISECONDS );

        // when
        boolean checkPointingNeeded = timeCheckPointThreshold.isCheckPointingNeeded( 42 );

        // then
        assertFalse( checkPointingNeeded );
    }

    @Test
    public void shouldBeTrueIfTimeThresholdIsReachedAndThereAreCommittedTransactions() throws Throwable
    {
        // given
        TimeCheckPointThreshold timeCheckPointThreshold = new TimeCheckPointThreshold( 100, clock );
        timeCheckPointThreshold.initialize( 2 );

        clock.forward( 100, MILLISECONDS );

        // when
        boolean checkPointingNeeded = timeCheckPointThreshold.isCheckPointingNeeded( 42 );

        // then
        assertTrue( checkPointingNeeded );
    }

    @Test
    public void shouldBeFalseIfTimeThresholdIsReachedButThereAreNoCommittedTransactions() throws Throwable
    {
        // given
        TimeCheckPointThreshold timeCheckPointThreshold = new TimeCheckPointThreshold( 100, clock );
        timeCheckPointThreshold.initialize( 42 );

        clock.forward( 100, MILLISECONDS );

        // when
        boolean checkPointingNeeded = timeCheckPointThreshold.isCheckPointingNeeded( 42 );

        // then
        assertFalse( checkPointingNeeded );
    }

    @Test
    public void shouldBeFalseIfTimeThresholdIsReachedAfterCheckPointHappenedButThereAreNoCommittedTransactions()
            throws Throwable
    {
        // given
        TimeCheckPointThreshold timeCheckPointThreshold = new TimeCheckPointThreshold( 100, clock );
        timeCheckPointThreshold.initialize( 2 );

        clock.forward( 100, MILLISECONDS );

        timeCheckPointThreshold.checkPointHappened( 42 );

        clock.forward( 100, MILLISECONDS );

        // when
        boolean checkPointingNeeded = timeCheckPointThreshold.isCheckPointingNeeded( 42 );

        // then
        assertFalse( checkPointingNeeded );
    }

    @Test
    public void shouldBeTrueIfTimeThresholdIsReachedAfterCheckPointHappenedAndThereAreCommittedTransactions()
            throws Throwable
    {
        // given
        TimeCheckPointThreshold timeCheckPointThreshold = new TimeCheckPointThreshold( 100, clock );
        timeCheckPointThreshold.initialize( 2 );

        clock.forward( 100, MILLISECONDS );

        timeCheckPointThreshold.checkPointHappened( 42 );

        clock.forward( 100, MILLISECONDS );

        // when
        boolean checkPointingNeeded = timeCheckPointThreshold.isCheckPointingNeeded( 43 );

        // then
        assertTrue( checkPointingNeeded );
    }
}
