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
package org.neo4j.kernel.impl.store.kvstore;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class RotationTimerFactoryTest
{
    @Test
    public void testTimer()
    {
        // GIVEN
        FakeClock fakeClock = Clocks.fakeClock( 10000 , TimeUnit.MILLISECONDS );

        // WHEN
        RotationTimerFactory timerFactory = new RotationTimerFactory( fakeClock, 1000 );
        RotationTimerFactory.RotationTimer timer = timerFactory.createTimer();
        RotationTimerFactory.RotationTimer anotherTimer = timerFactory.createTimer();

        // THEN
        assertFalse( timer.isTimedOut() );
        assertEquals( 0, timer.getElapsedTimeMillis() );
        assertNotSame( "Factory should construct different timers", timer, anotherTimer );

        // WHEN
        fakeClock = Clocks.fakeClock();
        RotationTimerFactory fakeTimerFactory = new RotationTimerFactory( fakeClock, 1000 );
        RotationTimerFactory.RotationTimer fakeTimer = fakeTimerFactory.createTimer();
        fakeClock.forward( 1001, TimeUnit.MILLISECONDS );

        assertTrue( fakeTimer.isTimedOut() );
        assertEquals( 1001, fakeTimer.getElapsedTimeMillis() );
    }
}
