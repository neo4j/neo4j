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
package org.neo4j.kernel.ha;

import java.util.List;

import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.paxos.PaxosClusterMemberAvailability;
import org.neo4j.cluster.member.paxos.PaxosClusterMemberEvents;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberContext;
import org.neo4j.kernel.ha.switchover.Switchover;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class ZooToPaxosSwitchover implements Switchover
{
    private final LifeSupport life;
    private final LifeSupport lifeToStart;
    private final List<Lifecycle> lifeToStop;
    private final DelegateInvocationHandler<ClusterMemberEvents> clusterEventsDelegateInvocationHandler;
    private final DelegateInvocationHandler<HighAvailabilityMemberContext> memberContextDelegateInvocationHandler;
    private final DelegateInvocationHandler clusterMemberAvailabilityDelegateInvocationHandler;
    private final ClusterMemberEvents localClusterEvents;
    private final HighAvailabilityMemberContext localMemberContext;
    private final PaxosClusterMemberAvailability localClusterMemberAvailability;

    public ZooToPaxosSwitchover( LifeSupport life, LifeSupport lifeToStart,
                                 List<Lifecycle> lifeToStop,
                                 DelegateInvocationHandler<ClusterMemberEvents>
                                         clusterEventsDelegateInvocationHandler,
                                 DelegateInvocationHandler<HighAvailabilityMemberContext>
                                         memberContextDelegateInvocationHandler,
                                 DelegateInvocationHandler clusterMemberAvailabilityDelegateInvocationHandler,
                                 PaxosClusterMemberEvents localClusterEvents,
                                 HighAvailabilityMemberContext localMemberContext, PaxosClusterMemberAvailability
            localClusterMemberAvailability )
    {
        this.life = life;
        this.lifeToStart = lifeToStart;
        this.lifeToStop = lifeToStop;
        this.clusterEventsDelegateInvocationHandler = clusterEventsDelegateInvocationHandler;
        this.memberContextDelegateInvocationHandler = memberContextDelegateInvocationHandler;
        this.clusterMemberAvailabilityDelegateInvocationHandler = clusterMemberAvailabilityDelegateInvocationHandler;
        this.localClusterEvents = localClusterEvents;
        this.localMemberContext = localMemberContext;
        this.localClusterMemberAvailability = localClusterMemberAvailability;
    }

    @Override
    public synchronized void doSwitchover()
    {
        try
        {
            for ( Lifecycle lifecycle : lifeToStop )
            {
                lifecycle.stop();
                lifecycle.shutdown();
            }
        }
        catch ( Throwable t )
        {
            throw new RuntimeException( t );
        }
        clusterEventsDelegateInvocationHandler.setDelegate( localClusterEvents );
        memberContextDelegateInvocationHandler.setDelegate( localMemberContext );
        clusterMemberAvailabilityDelegateInvocationHandler.setDelegate( localClusterMemberAvailability );
        life.add( lifeToStart );
        life.start();
    }
}
