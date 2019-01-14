/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.locking.ActiveLock.exclusiveLock;
import static org.neo4j.kernel.impl.locking.ActiveLock.sharedLock;
import static org.neo4j.kernel.impl.locking.ResourceTypes.LABEL;
import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;
import static org.neo4j.kernel.impl.locking.ResourceTypes.RELATIONSHIP;

@Ignore( "Not a test. This is a compatibility suite, run from LockingCompatibilityTestSuite." )
public class ActiveLocksListingCompatibility extends LockingCompatibilityTestSuite.Compatibility
{
    public ActiveLocksListingCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Test
    public void shouldListLocksHeldByTheCurrentClient()
    {
        // given
        clientA.acquireExclusive( LockTracer.NONE, NODE, 1, 2, 3 );
        clientA.acquireShared( LockTracer.NONE, NODE, 3, 4, 5 );

        // when
        Stream<? extends ActiveLock> locks = clientA.activeLocks();

        // then
        assertEquals(
                new HashSet<>( asList(
                        exclusiveLock( NODE, 1 ),
                        exclusiveLock( NODE, 2 ),
                        exclusiveLock( NODE, 3 ),
                        sharedLock( NODE, 3 ),
                        sharedLock( NODE, 4 ),
                        sharedLock( NODE, 5 ) ) ),
                locks.collect( toSet() ) );
    }

    @Test
    public void shouldCountNumberOfActiveLocks()
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
