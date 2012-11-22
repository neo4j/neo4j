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

import java.net.URI;

import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.ClusterMonitor;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;

public class MockedClusterMonitor implements ClusterMonitor, ClusterListener, BindingListener, HeartbeatListener
{
    private final MockedCluster cluster = new MockedCluster();
    private final MockedBinding binding = new MockedBinding();
    private final MockedHeartbeat heartbeat = new MockedHeartbeat();
    
    @Override
    public void failed( URI server )
    {
        heartbeat.failed( server );
    }

    @Override
    public void alive( URI server )
    {
        heartbeat.alive( server );
    }

    @Override
    public void addHeartbeatListener( HeartbeatListener listener )
    {
        heartbeat.addHeartbeatListener( listener );
    }

    @Override
    public void removeHeartbeatListener( HeartbeatListener listener )
    {
        heartbeat.removeHeartbeatListener( listener );
    }

    @Override
    public void listeningAt( URI me )
    {
        binding.listeningAt( me );
    }

    @Override
    public void addBindingListener( BindingListener listener )
    {
        binding.addBindingListener( listener );
    }

    @Override
    public void removeBindingListener( BindingListener listener )
    {
        binding.removeBindingListener( listener );
    }

    @Override
    public void enteredCluster( ClusterConfiguration clusterConfiguration )
    {
        cluster.enteredCluster( clusterConfiguration );
    }

    @Override
    public void leftCluster()
    {
        cluster.leftCluster();
    }

    @Override
    public void joinedCluster( URI member )
    {
        cluster.joinedCluster( member );
    }

    @Override
    public void leftCluster( URI member )
    {
        cluster.leftCluster( member );
    }

    @Override
    public void elected( String role, URI electedMember )
    {
        cluster.elected( role, electedMember );
    }

    @Override
    public void addClusterListener( ClusterListener listener )
    {
        cluster.addClusterListener( listener );
    }

    @Override
    public void removeClusterListener( ClusterListener listener )
    {
        cluster.removeClusterListener( listener );
    }
}
