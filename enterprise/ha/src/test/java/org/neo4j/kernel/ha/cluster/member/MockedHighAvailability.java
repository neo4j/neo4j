/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster.member;

import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.MASTER;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.PENDING;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.SLAVE;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.TO_MASTER;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.TO_SLAVE;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.ha.cluster.HighAvailability;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;

public class MockedHighAvailability implements HighAvailability, HighAvailabilityMemberListener
{
    private final List<HighAvailabilityMemberListener> listeners = new ArrayList<HighAvailabilityMemberListener>();
    
    @Override
    public void addHighAvailabilityMemberListener( HighAvailabilityMemberListener listener )
    {
        listeners.add( listener );
    }

    @Override
    public void removeHighAvailabilityMemberListener( HighAvailabilityMemberListener listener )
    {
        listeners.remove( listener );
    }

    @Override
    public void masterIsElected( final HighAvailabilityMemberChangeEvent event )
    {
        Listeners.notifyListeners( listeners, new Listeners.Notification<HighAvailabilityMemberListener>()
        {
            @Override
            public void notify( HighAvailabilityMemberListener listener )
            {
                listener.masterIsElected( event );
            }
        } );
    }

    @Override
    public void masterIsAvailable( final HighAvailabilityMemberChangeEvent event )
    {
        Listeners.notifyListeners( listeners, new Listeners.Notification<HighAvailabilityMemberListener>()
        {
            @Override
            public void notify( HighAvailabilityMemberListener listener )
            {
                listener.masterIsAvailable( event );
            }
        } );
    }

    @Override
    public void slaveIsAvailable( final HighAvailabilityMemberChangeEvent event )
    {
        Listeners.notifyListeners( listeners, new Listeners.Notification<HighAvailabilityMemberListener>()
        {
            @Override
            public void notify( HighAvailabilityMemberListener listener )
            {
                listener.slaveIsAvailable( event );
            }
        } );
    }

    @Override
    public void instanceStops( final HighAvailabilityMemberChangeEvent event )
    {
        Listeners.notifyListeners( listeners, new Listeners.Notification<HighAvailabilityMemberListener>()
        {
            @Override
            public void notify( HighAvailabilityMemberListener listener )
            {
                listener.instanceStops( event );
            }
        } );
    }
    
    public void masterIsElectedAndAvailable( URI clusterUri, URI haUri )
    {
        masterIsElected( clusterUri, haUri );
        masterIsAvailable( new HighAvailabilityMemberChangeEvent( TO_MASTER, MASTER, clusterUri, haUri ) );
    }
    
    public void slaveIsAvailable( URI clusterUri, URI haUri )
    {
        slaveIsAvailable( new HighAvailabilityMemberChangeEvent( TO_SLAVE, SLAVE, clusterUri, haUri ) );
    }

    public void masterIsElected( URI clusterUri, URI haUri )
    {
        masterIsElected( new HighAvailabilityMemberChangeEvent( PENDING, TO_MASTER, clusterUri, haUri ) );
    }
}
