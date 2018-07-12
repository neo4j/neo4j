/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.net;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DefaultNetworkConnectionTrackerTest
{
    private final TrackedNetworkConnection connection1 = connectionMock( "1" );
    private final TrackedNetworkConnection connection2 = connectionMock( "2" );
    private final TrackedNetworkConnection connection3 = connectionMock( "3" );

    private final NetworkConnectionTracker tracker = new DefaultNetworkConnectionTracker();

    @Test
    void shouldCreateIds()
    {
        assertEquals( "bolt-0", tracker.newConnectionId( "bolt" ) );
        assertEquals( "bolt-1", tracker.newConnectionId( "bolt" ) );
        assertEquals( "bolt-2", tracker.newConnectionId( "bolt" ) );

        assertEquals( "http-3", tracker.newConnectionId( "http" ) );
        assertEquals( "http-4", tracker.newConnectionId( "http" ) );

        assertEquals( "https-5", tracker.newConnectionId( "https" ) );
        assertEquals( "https-6", tracker.newConnectionId( "https" ) );
        assertEquals( "https-7", tracker.newConnectionId( "https" ) );
    }

    @Test
    void shouldAllowAddingOfConnections()
    {
        tracker.add( connection1 );
        tracker.add( connection2 );
        tracker.add( connection3 );

        assertThat( tracker.activeConnections(), containsInAnyOrder( connection1, connection2, connection3 ) );
    }

    @Test
    void shouldFailWhenAddingConnectionWithSameId()
    {
        tracker.add( connection1 );

        assertThrows( IllegalArgumentException.class, () -> tracker.add( connection1 ) );
    }

    @Test
    void shouldRemoveConnections()
    {
        tracker.add( connection1 );
        tracker.add( connection2 );
        tracker.add( connection3 );

        tracker.remove( connection2 );
        assertThat( tracker.activeConnections(), containsInAnyOrder( connection1, connection3 ) );

        tracker.remove( connection1 );
        assertThat( tracker.activeConnections(), containsInAnyOrder( connection3 ) );
    }

    @Test
    void shouldDoNothingWhenRemovingUnknownConnection()
    {
        tracker.add( connection1 );
        tracker.add( connection3 );
        assertThat( tracker.activeConnections(), containsInAnyOrder( connection1, connection3 ) );

        tracker.remove( connection2 );

        assertThat( tracker.activeConnections(), containsInAnyOrder( connection1, connection3 ) );
    }

    @Test
    void shouldGetKnownConnectionById()
    {
        tracker.add( connection1 );
        tracker.add( connection2 );

        assertEquals( connection1, tracker.get( connection1.id() ) );
        assertEquals( connection2, tracker.get( connection2.id() ) );
    }

    @Test
    void shouldReturnNullForUnknownId()
    {
        tracker.add( connection1 );

        assertNotNull( tracker.get( connection1.id() ) );
        assertNull( tracker.get( connection2.id() ) );
        assertNull( tracker.get( connection3.id() ) );
    }

    @Test
    void shouldListActiveConnectionsWhenEmpty()
    {
        assertThat( tracker.activeConnections(), empty() );
    }

    private static TrackedNetworkConnection connectionMock( String id )
    {
        TrackedNetworkConnection connection = mock( TrackedNetworkConnection.class );
        when( connection.id() ).thenReturn( id );
        return connection;
    }
}
