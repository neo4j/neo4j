/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.ha.cluster.member;

import java.util.function.Predicate;

import org.neo4j.cluster.InstanceId;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher;

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
        return item -> item.hasRole( role );
    }

    public static Predicate<ClusterMember> hasInstanceId( final InstanceId instanceId )
    {
        return item -> item.getInstanceId().equals( instanceId );
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
        return Iterables.map(
                member -> currentMember.getInstanceId().equals( member.getInstanceId() ) ? currentMember : member, members );
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
