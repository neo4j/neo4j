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
package org.neo4j.internal.id.indexed;

import org.apache.commons.lang3.mutable.MutableInt;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.test.Race;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.test.Race.throwing;

class ScanLockTest
{
    @Test
    void optimisticLockShouldNotStarveLockRequest()
    {
        // given
        ScanLock lock = ScanLock.lockFreeAndOptimistic();
        AtomicInteger numberOfLocks = new AtomicInteger();
        int targetNumberOfLocks = 10;
        Race race = new Race().withMaxDuration( 10, TimeUnit.SECONDS ).withEndCondition( () -> numberOfLocks.get() >= targetNumberOfLocks );
        race.addContestants( 1, throwing( () ->
        {
            if ( lock.tryLock() )
            {
                try
                {
                    LockSupport.parkNanos( 500_000 );
                }
                finally
                {
                    lock.unlock();
                }
            }
        } ) );
        race.addContestant( throwing( () ->
        {
            lock.lock();
            lock.unlock();
            numberOfLocks.incrementAndGet();
        } ) );
        race.goUnchecked();

        // then if should be >= 10, which is grossly under-estimated tho, it should get a couple of hundred thousand
        assertThat( numberOfLocks.intValue(), Matchers.greaterThanOrEqualTo( targetNumberOfLocks ) );
    }
}
