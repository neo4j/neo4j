/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

package org.neo4j.kernel.impl.enterprise;

import org.junit.Test;

import org.neo4j.kernel.api.bolt.ManagedBoltStateMachine;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import static org.neo4j.helpers.collection.Iterators.asSet;

public class StandardBoltConnectionTrackerTest
{
    @Test
    public void shouldTrackConnectionsAndTheirOwners() throws Exception
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
