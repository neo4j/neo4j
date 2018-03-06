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

public class MaximumTotalRetriesTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldRetryUntilMaximumRetries() throws Exception
    {
        FakeClock clock = new FakeClock();
        MaximumTotalRetries maximumTotalRetries = new MaximumTotalRetries( 4, -1, clock );

        maximumTotalRetries.assertContinue();
        maximumTotalRetries.assertContinue();
        maximumTotalRetries.assertContinue();
        expectedException.expect( StoreCopyFailedException.class );
        maximumTotalRetries.assertContinue();
    }

    @Test
    public void shouldContinueIfAllowedInBetweenTimeIsMet() throws Exception
    {
        // given
        FakeClock clock = new FakeClock();
        MaximumTotalRetries maximumTotalRetries = new MaximumTotalRetries( 1, 0, clock );

        // when we retry
        maximumTotalRetries.assertContinue();

        // then we can retry again because in between time == 0
        maximumTotalRetries.assertContinue();

        //when we increment clock
        clock.forward( 1, TimeUnit.MILLISECONDS );

        //then we expected exception thrown
        expectedException.expect( StoreCopyFailedException.class );

        // when we retry
        maximumTotalRetries.assertContinue();
    }
}
