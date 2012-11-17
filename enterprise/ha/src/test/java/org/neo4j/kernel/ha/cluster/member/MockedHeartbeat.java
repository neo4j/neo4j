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

import static org.neo4j.helpers.Listeners.notifyListeners;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cluster.protocol.heartbeat.Heartbeat;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.helpers.Listeners;

public class MockedHeartbeat implements Heartbeat, HeartbeatListener
{
    private final List<HeartbeatListener> listeners = new ArrayList<HeartbeatListener>();
    
    @Override
    public void addHeartbeatListener( HeartbeatListener listener )
    {
        listeners.add( listener );
    }

    @Override
    public void removeHeartbeatListener( HeartbeatListener listener )
    {
        listeners.remove( listener );
    }
    
    @Override
    public void failed( final URI server )
    {
        notifyListeners( listeners, new Listeners.Notification<HeartbeatListener>()
        {
            @Override
            public void notify( HeartbeatListener listener )
            {
                listener.failed( server );
            }
        } );
    }

    @Override
    public void alive( final URI server )
    {
        notifyListeners( listeners, new Listeners.Notification<HeartbeatListener>()
        {
            @Override
            public void notify( HeartbeatListener listener )
            {
                listener.alive( server );
            }
        } );
    }
}
