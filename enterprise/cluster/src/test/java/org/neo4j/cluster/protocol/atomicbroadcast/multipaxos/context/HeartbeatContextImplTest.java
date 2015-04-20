/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;

import org.neo4j.cluster.DelayedDirectExecutor;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ClusterProtocolAtomicbroadcastTestUtil.ids;
import static org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ClusterProtocolAtomicbroadcastTestUtil.members;

public class HeartbeatContextImplTest
{
    @Test
    public void shouldFailAndAliveBothNotifyHeartbeatListenerInDelayedDirectExecutor() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId failedMachine = new InstanceId( 2 );
        InstanceId goodMachine = new InstanceId( 3 );

        Logging log = mock( Logging.class );
        when( log.getMessagesLog( HeartbeatContext.class ) ).thenReturn( mock( StringLogger.class ) );
        Timeouts timeouts = mock( Timeouts.class );

        CommonContextState commonState = mock( CommonContextState.class );
        ClusterConfiguration configuration = mock (ClusterConfiguration.class);
        when( commonState.configuration() ).thenReturn( configuration );
        when( configuration.getMembers() ).thenReturn( members( 3 ) );
        when( configuration.getMemberIds() ).thenReturn( ids( 3 ) );

        final List<Runnable> runnables = new ArrayList<Runnable>();
        HeartbeatContext context = new HeartbeatContextImpl( me, commonState, log, timeouts, new DelayedDirectExecutor(
                log )
        {
            @Override
            public synchronized void execute( Runnable command )
            {
                runnables.add( command );
            }
        } );
        context.addHeartbeatListener( mock( HeartbeatListener.Adapter.class ) );

        context.suspicions( goodMachine, new HashSet<InstanceId>( Arrays.asList( failedMachine ) ) );
        context.suspect( failedMachine ); // fail
        context.alive( failedMachine ); // alive

        // Then
        assertEquals( 2, runnables.size() ); // fail + alive
    }
}
