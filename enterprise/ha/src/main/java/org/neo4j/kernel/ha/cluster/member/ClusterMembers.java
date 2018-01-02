/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cluster.InstanceId;
import org.neo4j.function.Function;
import org.neo4j.function.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;

import static java.lang.String.format;

/**
 * Keeps a list of members, their roles and availability for display for example in JMX or REST.
 * <p>
 * Member state info is based on {@link ObservedClusterMembers} and {@link HighAvailabilityMemberStateMachine}.
 * State of the current member is always valid, all other instances are only 'best effort'.
 */
public class ClusterMembers
{
    public static Predicate<ClusterMember> inRole( final String role )
    {
        return new Predicate<ClusterMember>()
        {
            @Override
            public boolean test( ClusterMember item )
            {
                return item.hasRole( role );
            }
        };
    }

    public static Predicate<ClusterMember> hasInstanceId( final InstanceId instanceId )
    {
        return new Predicate<ClusterMember>()
        {
            @Override
            public boolean test( ClusterMember item )
            {
                return item.getInstanceId().equals( instanceId );
            }
        };
    }

    private final ObservedClusterMembers observedClusterMembers;
    private final HighAvailabilityMemberStateMachine stateMachine;

    public ClusterMembers( ObservedClusterMembers observedClusterMembers,
            HighAvailabilityMemberStateMachine stateMachine )
    {
        this.observedClusterMembers = observedClusterMembers;
        this.stateMachine = stateMachine;
    }

    public ClusterMember getCurrentMember()
    {
        ClusterMember currentMember = observedClusterMembers.getCurrentMember();
        if ( currentMember == null )
        {
            return null;
        }
        HighAvailabilityMemberState currentState = stateMachine.getCurrentState();
        return updateRole( currentMember, currentState );
    }

    public String getCurrentMemberRole()
    {
        ClusterMember currentMember = getCurrentMember();
        return (currentMember == null) ? HighAvailabilityModeSwitcher.UNKNOWN : currentMember.getHARole();
    }

    public Iterable<ClusterMember> getMembers()
    {
        return getActualMembers( observedClusterMembers.getMembers() );
    }

    public Iterable<ClusterMember> getAliveMembers()
    {
        return getActualMembers( observedClusterMembers.getAliveMembers() );
    }

    private Iterable<ClusterMember> getActualMembers( Iterable<ClusterMember> members )
    {
        final ClusterMember currentMember = getCurrentMember();
        if ( currentMember == null )
        {
            return members;
        }
        return Iterables.map( new Function<ClusterMember,ClusterMember>()
        {
            @Override
            public ClusterMember apply( ClusterMember member ) throws RuntimeException
            {
                return currentMember.getInstanceId().equals( member.getInstanceId() ) ? currentMember : member;
            }
        }, members );
    }

    private static ClusterMember updateRole( ClusterMember member, HighAvailabilityMemberState state )
    {
        switch ( state )
        {
        case MASTER:
            return member.availableAs( HighAvailabilityModeSwitcher.MASTER, member.getHAUri(), member.getStoreId() );
        case SLAVE:
            return member.availableAs( HighAvailabilityModeSwitcher.SLAVE, member.getHAUri(), member.getStoreId() );
        default:
            return member.unavailable();
        }
    }

    @Override
    public String toString()
    {
        StringBuilder buf = new StringBuilder();
        for ( ClusterMember clusterMember : getMembers() )
        {
            buf.append( "  " ).append( clusterMember.getInstanceId() ).append( ":" )
               .append( clusterMember.getHARole() )
               .append( " (is alive = " ).append( clusterMember.isAlive() ).append( ")" )
               .append( format( "%n" ) );
        }
        return buf.toString();
    }
}
