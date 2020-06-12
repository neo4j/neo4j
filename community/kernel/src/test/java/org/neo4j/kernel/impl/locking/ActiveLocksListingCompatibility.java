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
package org.neo4j.kernel.impl.locking;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.stream.Stream;

import org.neo4j.lock.LockTracer;
import org.neo4j.lock.LockType;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.lock.ResourceTypes.LABEL;
import static org.neo4j.lock.ResourceTypes.NODE;
import static org.neo4j.lock.ResourceTypes.RELATIONSHIP;

abstract class ActiveLocksListingCompatibility extends LockCompatibilityTestSupport
{
    ActiveLocksListingCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Test
    void shouldListLocksHeldByTheCurrentClient()
    {
        // given
        clientA.acquireExclusive( LockTracer.NONE, NODE, 1, 2, 3 );
        clientA.acquireShared( LockTracer.NONE, NODE, 3, 4, 5 );

        // when
        Stream<ActiveLock> locks = clientA.activeLocks();

        // then
        assertEquals(
                new HashSet<>( asList(
                        new ActiveLock( NODE, LockType.EXCLUSIVE, 1 ),
                        new ActiveLock( NODE, LockType.EXCLUSIVE, 2 ),
                        new ActiveLock( NODE, LockType.EXCLUSIVE, 3 ),
                        new ActiveLock( NODE, LockType.SHARED, 3 ),
                        new ActiveLock( NODE, LockType.SHARED, 4 ),
                        new ActiveLock( NODE, LockType.SHARED, 5 ) ) ),
                locks.collect( toSet() ) );
    }

    @Test
    void shouldCountNumberOfActiveLocks()
    {
        // given
        clientA.acquireShared( LockTracer.NONE, LABEL, 0 );
        clientA.acquireShared( LockTracer.NONE, RELATIONSHIP, 17 );
        clientA.acquireShared( LockTracer.NONE, NODE, 12 );

        // when
        long count = clientA.activeLockCount();

        // then
        assertEquals( 3, count );
    }
}
