/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.util.HashMap;
import java.util.Map;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.heartbeat.Heartbeat;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;

/**
 * Keeps an up to date list of members, their roles and availability for
 * display for example in JMX.
 */
public class ClusterMembers
{
    public static final Predicate<ClusterMember> ALIVE = new Predicate<ClusterMember>()
    {
        @Override
        public boolean accept( ClusterMember item )
        {
            return item.isAlive();
        }
    };

    private final InstanceId me;

    public static Predicate<ClusterMember> inRole( final String role )
    {
        return new Predicate<ClusterMember>()
        {
            @Override
            public boolean accept( ClusterMember item )
            {
                return item.hasRole( role );
            }
        };
    }

    private final Map<InstanceId, ClusterMember> members = new CopyOnWriteHashMap<InstanceId, ClusterMember>();

    public ClusterMembers( Cluster cluster, Heartbeat heartbeat, ClusterMemberEvents clusterMemberEvents,
                           InstanceId me )
    {
        this.me = me;
        cluster.addClusterListener( new HAMClusterListener() );
        heartbeat.addHeartbeatListener( new HAMHeartbeatListener() );
        clusterMemberEvents.addClusterMemberListener( new HAMClusterMemberListener() );
    }

    public Iterable<ClusterMember> getMembers()
    {
        return members.values();
    }

    public ClusterMember getSelf()
    {
        for ( ClusterMember clusterMember : getMembers() )
        {
            if ( clusterMember.getMemberId().equals( me ) )
            {
                return clusterMember;
            }
        }
        return null;
    }

    private ClusterMember getMember( InstanceId server )
    {
        ClusterMember clusterMember = members.get( server );
        if ( clusterMember == null )
            throw new IllegalStateException( "Member " + server + " not found in " + members );
        return clusterMember;
    }
    
    private class HAMClusterListener extends ClusterListener.Adapter
    {
        @Override
        public void enteredCluster( ClusterConfiguration configuration )
        {
            Map<InstanceId, ClusterMember> newMembers = new HashMap<InstanceId, ClusterMember>();
            for ( InstanceId memberClusterUri : configuration.getMembers().keySet() )
                newMembers.put( memberClusterUri, new ClusterMember( memberClusterUri ) );
            members.clear();
            members.putAll( newMembers );
        }

        @Override
        public void leftCluster()
        {
            members.clear();
        }

        @Override
        public void joinedCluster( InstanceId member, URI memberUri )
        {
            members.put( member, new ClusterMember( member ) );
        }

        @Override
        public void leftCluster( InstanceId member )
        {
            members.remove( member );
        }
    }

    private class HAMClusterMemberListener extends ClusterMemberListener.Adapter
    {
        private InstanceId masterId = null;

        @Override
        public void coordinatorIsElected( InstanceId coordinatorId )
        {
            if ( coordinatorId.equals( this.masterId ) )
            {
                return;
            }
            this.masterId = coordinatorId;
            Map<InstanceId, ClusterMember> newMembers = new CopyOnWriteHashMap<InstanceId, ClusterMember>();
            for ( Map.Entry<InstanceId, ClusterMember> memberEntry : members.entrySet() )
            {
                newMembers.put( memberEntry.getKey(), memberEntry.getValue().unavailableAs(
                        HighAvailabilityModeSwitcher.MASTER ).unavailableAs( HighAvailabilityModeSwitcher.SLAVE ) );
            }
            members.clear();
            members.putAll( newMembers );
        }

        @Override
        public void memberIsAvailable( String role, InstanceId instanceId, URI roleUri )
        {
            members.put( instanceId, getMember( instanceId ).availableAs( role, roleUri ) );
        }

        @Override
        public void memberIsUnavailable( String role, InstanceId unavailableId )
        {
            ClusterMember member = null;
            try
            {
                member = getMember( unavailableId );
                members.put( unavailableId, member.unavailableAs( role ) );
            }
            catch ( IllegalStateException e )
            {
                // Unknown member
            }
        }
    }

    private class HAMHeartbeatListener extends HeartbeatListener.Adapter
    {
        @Override
        public void failed( InstanceId server )
        {
            if (members.containsKey( server ))
                members.put( server, getMember( server ).failed() );
        }

        @Override
        public void alive( InstanceId server )
        {
            if (members.containsKey( server ))
                members.put( server, getMember( server ).alive() );
        }
    }
}
