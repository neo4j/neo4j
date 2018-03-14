/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.TimeUnit;

import org.neo4j.time.FakeClock;

public class MaximumTotalTimeTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldFailWhenAllowedTimeHasPassed() throws StoreCopyFailedException
    {
        TimeUnit timeUnit = TimeUnit.SECONDS;
        FakeClock fakeClock = new FakeClock( 0, timeUnit );

        MaximumTotalTime maximumTotalTime = new MaximumTotalTime( 5, timeUnit, fakeClock );

        maximumTotalTime.assertContinue();
        fakeClock.forward( 5, timeUnit );
        maximumTotalTime.assertContinue();
        expectedException.expect( StoreCopyFailedException.class );
        fakeClock.forward( 1, timeUnit );
        maximumTotalTime.assertContinue();
    }
}
