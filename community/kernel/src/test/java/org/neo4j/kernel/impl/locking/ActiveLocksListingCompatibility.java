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
package org.neo4j.kernel.impl.locking;

import java.util.Collection;

import org.junit.Ignore;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;

@Ignore( "Not a test. This is a compatibility suite, run from LockingCompatibilityTestSuite." )
public class ActiveLocksListingCompatibility extends LockingCompatibilityTestSuite.Compatibility
{
    public ActiveLocksListingCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Test
    public void shouldListLocksHeldByTheCurrentClient() throws Exception
    {
        // given
        clientA.acquireExclusive( LockTracer.NONE, NODE, 1, 2, 3 );
        clientA.acquireShared( LockTracer.NONE, NODE, 3, 4, 5 );

        // when
        Collection<Locks.ActiveLock> locks = clientA.activeLocks();

        // then
        assertEquals(
                asList(
                        new Locks.ActiveExclusiveLock( NODE, 1 ),
                        new Locks.ActiveExclusiveLock( NODE, 2 ),
                        new Locks.ActiveExclusiveLock( NODE, 3 ),
                        new Locks.ActiveSharedLock( NODE, 3 ),
                        new Locks.ActiveSharedLock( NODE, 4 ),
                        new Locks.ActiveSharedLock( NODE, 5 ) ),
                locks );
    }
}
