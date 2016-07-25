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
package org.neo4j.coreedge.discovery;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

class MembershipListenerAdapter implements MembershipListener
{
    private final List<CoreTopologyService.Listener> listeners = new ArrayList<>();
    private final Log log;
    private ClusterTopology currentTopology = new ClusterTopology( false, emptyMap(), emptySet() );
    private String membershipRegistrationId;
    private HazelcastInstance hazelcastInstance;

    MembershipListenerAdapter( LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
    }

    void addMembershipListener( CoreTopologyService.Listener listener )
    {
        listeners.add( listener );
    }

    @Override
    public void memberAdded( MembershipEvent membershipEvent )
    {
        log.info( "Member added %s", membershipEvent );
        topologyChanged();
    }

    @Override
    public void memberRemoved( MembershipEvent membershipEvent )
    {
        log.info( "Member removed %s", membershipEvent );
        topologyChanged();
    }

    @Override
    public void memberAttributeChanged( MemberAttributeEvent memberAttributeEvent )
    {
    }

    public ClusterTopology currentTopology()
    {
        return currentTopology;
    }

    void attach( HazelcastInstance hazelcastInstance )
    {
        this.hazelcastInstance = hazelcastInstance;
        membershipRegistrationId = hazelcastInstance.getCluster().addMembershipListener( this );
        topologyChanged();
    }

    void detach()
    {
        hazelcastInstance.getCluster().removeMembershipListener( membershipRegistrationId );
    }

    private void topologyChanged()
    {
        currentTopology = HazelcastClusterTopology.fromHazelcastInstance( hazelcastInstance, log );
        listeners.forEach( CoreTopologyService.Listener::onTopologyChange );
        log.info( "Current topology is %s.", currentTopology );
    }
}
