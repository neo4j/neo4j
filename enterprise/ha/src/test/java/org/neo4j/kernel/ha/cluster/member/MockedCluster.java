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
import java.util.concurrent.Future;

import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.helpers.Listeners;

public class MockedCluster implements Cluster, ClusterListener
{
    private final List<ClusterListener> listeners = new ArrayList<ClusterListener>();
    
    @Override
    public void addClusterListener( ClusterListener listener )
    {
        listeners.add( listener );
    }

    @Override
    public void removeClusterListener( ClusterListener listener )
    {
        listeners.remove( listener );
    }
    
    @Override
    public void enteredCluster( final ClusterConfiguration clusterConfiguration )
    {
        notifyListeners( listeners, new Listeners.Notification<ClusterListener>()
        {
            @Override
            public void notify( ClusterListener listener )
            {
                listener.enteredCluster( clusterConfiguration );
            }
        } );
    }

    @Override
    public void leftCluster()
    {
        notifyListeners( listeners, new Listeners.Notification<ClusterListener>()
        {
            @Override
            public void notify( ClusterListener listener )
            {
                listener.leftCluster();
            }
        } );
    }

    @Override
    public void joinedCluster( final URI member )
    {
        notifyListeners( listeners, new Listeners.Notification<ClusterListener>()
        {
            @Override
            public void notify( ClusterListener listener )
            {
                listener.joinedCluster( member );
            }
        } );
    }

    @Override
    public void leftCluster( final URI member )
    {
        notifyListeners( listeners, new Listeners.Notification<ClusterListener>()
        {
            @Override
            public void notify( ClusterListener listener )
            {
                listener.leftCluster( member );
            }
        } );
    }

    @Override
    public void elected( final String role, final URI electedMember )
    {
        notifyListeners( listeners, new Listeners.Notification<ClusterListener>()
        {
            @Override
            public void notify( ClusterListener listener )
            {
                listener.elected( role, electedMember );
            }
        } );
    }

    @Override
    public void create( String clusterName )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<ClusterConfiguration> join( URI otherServerUrl )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void leave()
    {
        throw new UnsupportedOperationException();
    }
}
