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

import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.cluster.ClusterMonitor;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.ha.cluster.HighAvailability;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.management.ClusterMemberInfo;

/**
 * Keeps an up to date list of members, their roles and availability for
 * display for example in JMX.
 * 
 * @author Mattias Persson
 */
public class HighAvailabilityMembers extends AbstractHighAvailabilityMembers
{
    private final Map<URI, Member> members = new CopyOnWriteHashMap<URI, Member>();
    private ClusterConfiguration clusterConfiguration;
    
    public HighAvailabilityMembers( ClusterMonitor clusterMonitor, HighAvailability highAvailability )
    {
        super( clusterMonitor, highAvailability );
        clusterMonitor.addClusterListener( new LocalClusterListener() );
        clusterMonitor.addHeartbeatListener( new LocalHeartbeatListener() );
    }
    
    public Iterable<ClusterMemberInfo> getMembers()
    {
        return map( new Function<Member, ClusterMemberInfo>()
        {
            @Override
            public ClusterMemberInfo apply( Member from )
            {
                return from.asClusterMemberInfo();
            }
        }, members.values() );
    }
    
    private class LocalHeartbeatListener extends HeartbeatListener.Adapter
    {
        @Override
        public void failed( URI server )
        {
            members.put( server, getMember( server ).setAlive( false ) );
        }
        
        @Override
        public void alive( URI server )
        {
            members.put( server, getMember( server ).setAlive( true ) );
        }
    }
    
    private class LocalClusterListener extends ClusterListener.Adapter
    {
        @Override
        public void enteredCluster( ClusterConfiguration configuration )
        {
            clusterConfiguration = configuration;
            initializeMembersFromClusterConfiguration( configuration );
        }

        @Override
        public void leftCluster()
        {
            members.remove( getMyServerUri() );
        }

        @Override
        public void joinedCluster( URI member )
        {
            members.put( member, new Member( member ) );
        }

        @Override
        public void leftCluster( URI member )
        {
            members.remove( member );
        }
    }
    
    private enum HighAvailabilityRole
    {
        UNKNOWN,
        SLAVE,
        MASTER;
    }
    
    private class Member
    {
        private final URI clusterUri;
        private final HighAvailabilityRole haRole;
        private final URI haUri;
        private final boolean available;
        private final boolean alive;
        
        public Member( URI clusterUri )
        {
            this( clusterUri, HighAvailabilityRole.UNKNOWN, null, false, true );
        }
        
        private Member( URI clusterUri, HighAvailabilityRole haRole, URI haUri, boolean available, boolean alive )
        {
            this.clusterUri = clusterUri;
            this.haRole = haRole;
            this.haUri = haUri;
            this.available = available;
            this.alive = alive;
        }

        protected ClusterMemberInfo asClusterMemberInfo()
        {
            String[] clusterRoles = asCollection( clusterConfiguration.getRolesOf( clusterUri ) ).toArray( new String[0] );
            return new ClusterMemberInfo( clusterUri.toString(), available, alive, haRole.name(),
                    clusterRoles, new String[] { clusterUri.toString(), nullSafeUriToString( haUri ) } );
        }
        
        Member becomeAvailable( HighAvailabilityRole haRole, URI haUri )
        {
            return new Member( this.clusterUri, haRole, haUri, true, this.alive );
        }
        
        Member setAlive( boolean alive )
        {
            return new Member( this.clusterUri, this.haRole, this.haUri, this.available, alive );
        }
        
        @Override
        public String toString()
        {
            return asClusterMemberInfo().toString();
        }
    }
    
    private void initializeMembersFromClusterConfiguration( ClusterConfiguration configuration )
    {
        Map<URI, Member> newMembers = new HashMap<URI, Member>();
        for ( URI memberClusterUri : clusterConfiguration.getMembers() )
            newMembers.put( memberClusterUri, new Member( memberClusterUri ) );
        members.clear();
        members.putAll( newMembers );
    }
    
    @Override
    protected void slaveIsAvailable( URI serverClusterUri, URI serverHaUri, boolean iAmMaster )
    {
        members.put( serverClusterUri, getMember( serverClusterUri )
                .becomeAvailable( HighAvailabilityRole.SLAVE, serverHaUri ) );
    }
    
    @Override
    protected void masterIsAvailable( URI serverClusterUri, URI serverHaUri, boolean iAmMaster )
    {
        members.put( serverClusterUri, getMember( serverClusterUri )
                .becomeAvailable( HighAvailabilityRole.MASTER, serverHaUri ) );
    }
    
    @Override
    protected void newMasterElected()
    {
        initializeMembersFromClusterConfiguration( clusterConfiguration );
    }

    protected Member getMember( URI server )
    {
        Member member = members.get( server );
        if ( member == null )
            throw new IllegalStateException( "Member " + server + " not found in " + members );
        return member;
    }
    
    private static String nullSafeUriToString( URI uri )
    {
        return uri != null ? uri.toString() : null;
    }
}
