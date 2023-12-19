/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.enterprise;

import org.junit.Test;

import org.neo4j.kernel.api.bolt.ManagedBoltStateMachine;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import static org.neo4j.helpers.collection.Iterators.asSet;

public class StandardBoltConnectionTrackerTest
{
    @Test
    public void shouldTrackConnectionsAndTheirOwners()
    {
        // given
        StandardBoltConnectionTracker tracker = new StandardBoltConnectionTracker();
        ManagedBoltStateMachine owner1machine1 = mock( ManagedBoltStateMachine.class );
        ManagedBoltStateMachine owner1machine2 = mock( ManagedBoltStateMachine.class );
        ManagedBoltStateMachine owner2machine1 = mock( ManagedBoltStateMachine.class );
        ManagedBoltStateMachine owner2machine2 = mock( ManagedBoltStateMachine.class );
        tracker.onRegister( owner1machine1, "owner1" );
        tracker.onRegister( owner1machine2, "owner1" );
        tracker.onRegister( owner2machine1, "owner2" );
        tracker.onRegister( owner2machine2, "owner2" );

        // then
        assertEquals( asSet( owner1machine1, owner1machine2, owner2machine1, owner2machine2 ),
                tracker.getActiveConnections() );
        assertEquals( asSet( owner1machine1, owner1machine2 ), tracker.getActiveConnections( "owner1" ) );
        assertEquals( asSet( owner2machine1, owner2machine2 ), tracker.getActiveConnections( "owner2" ) );
    }
}
