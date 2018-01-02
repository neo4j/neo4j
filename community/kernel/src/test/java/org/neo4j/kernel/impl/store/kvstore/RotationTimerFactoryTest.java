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
package org.neo4j.kernel.impl.store.kvstore;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.FakeClock;
import org.neo4j.helpers.FrozenClock;

public class RotationTimerFactoryTest
{
    @Test
    public void testTimer() throws Exception
    {
        // GIVEN
        FrozenClock clock = new FrozenClock( 10000, TimeUnit.MILLISECONDS );

        // WHEN
        RotationTimerFactory timerFactory = new RotationTimerFactory( clock, 1000);
        RotationTimerFactory.RotationTimer timer = timerFactory.createTimer();
        RotationTimerFactory.RotationTimer anotherTimer = timerFactory.createTimer();

        // THEN
        Assert.assertFalse( timer.isTimedOut() );
        Assert.assertEquals( 0, timer.getElapsedTimeMillis() );
        Assert.assertNotSame( "Factory should construct different timers", timer, anotherTimer );

        // WHEN
        FakeClock fakeClock = new FakeClock();
        RotationTimerFactory fakeTimerFactory = new RotationTimerFactory( fakeClock, 1000);
        RotationTimerFactory.RotationTimer fakeTimer = fakeTimerFactory.createTimer();
        fakeClock.forward( 1001, TimeUnit.MILLISECONDS );

        Assert.assertTrue( fakeTimer.isTimedOut() );
        Assert.assertEquals( 1001, fakeTimer.getElapsedTimeMillis());
    }
}
