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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.cluster.DelayedDirectExecutor;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.logging.NullLogProvider;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ClusterProtocolAtomicbroadcastTestUtil.ids;
import static org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ClusterProtocolAtomicbroadcastTestUtil.members;

public class HeartbeatContextImplTest
{
    @Test
    public void shouldFailAndAliveBothNotifyHeartbeatListenerInDelayedDirectExecutor()
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId failedMachine = new InstanceId( 2 );
        InstanceId goodMachine = new InstanceId( 3 );

        Timeouts timeouts = mock( Timeouts.class );

        CommonContextState commonState = mock( CommonContextState.class );
        ClusterConfiguration configuration = mock( ClusterConfiguration.class );
        when( commonState.configuration() ).thenReturn( configuration );
        when( configuration.getMembers() ).thenReturn( members( 3 ) );
        when( configuration.getMemberIds() ).thenReturn( ids( 3 ) );

        final List<Runnable> runnables = new ArrayList<>();
        HeartbeatContext context =
                new HeartbeatContextImpl( me, commonState, NullLogProvider.getInstance(), timeouts,
                    new DelayedDirectExecutor( NullLogProvider.getInstance() )
        {
            @Override
            public synchronized void execute( Runnable command )
            {
                runnables.add( command );
            }
        } );
        context.addHeartbeatListener( mock( HeartbeatListener.class ) );

        context.suspicions( goodMachine, new HashSet<>( singletonList( failedMachine ) ) );
        context.suspect( failedMachine ); // fail
        context.alive( failedMachine ); // alive

        // Then
        assertEquals( 2, runnables.size() ); // fail + alive
    }

    @Test
    public void shouldFailAllInstancesIfAllOtherInstancesAreSuspected()
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId member2 = new InstanceId( 2 );
        InstanceId member3 = new InstanceId( 3 );

        Timeouts timeouts = mock( Timeouts.class );

        CommonContextState commonState = mock( CommonContextState.class );
        ClusterConfiguration configuration = mock( ClusterConfiguration.class );
        when( commonState.configuration() ).thenReturn( configuration );
        when( configuration.getMembers() ).thenReturn( members( 3 ) );
        when( configuration.getMemberIds() ).thenReturn( ids( 3 ) );

        DelayedDirectExecutor executor = new DelayedDirectExecutor( NullLogProvider.getInstance() );
        HeartbeatContext context =
                new HeartbeatContextImpl( me, commonState, NullLogProvider.getInstance(), timeouts,
                        executor );

        List<InstanceId> failed = new ArrayList<>( 2 );
        HeartbeatListener listener = new HeartbeatListener()
        {
            @Override
            public void failed( InstanceId server )
            {
                failed.add( server );
            }

            @Override
            public void alive( InstanceId server )
            {
                failed.remove( server );
            }
        };

        context.addHeartbeatListener( listener );

        // when
        // just one suspicion comes, no extra failing action should be taken
        context.suspect( member2 );
        executor.drain();

        // then
        assertEquals( 0, failed.size() );

        // when
        // the other instance is suspected, all instances must be marked as failed
        context.suspect( member3 );
        executor.drain();

        // then
        assertEquals( 2, failed.size() );
        assertTrue( failed.contains( member2 ) );
        assertTrue( failed.contains( member3 ) );

        // when
        // one of them comes alive again, only that instance should be marked as alive
        context.alive( member2 );
        executor.drain();

        // then
        assertEquals( 1, failed.size() );
        assertTrue( failed.contains( member3 ) );
    }

    @Test
    public void majorityOfNonSuspectedInstancesShouldBeEnoughToMarkAnInstanceAsFailed()
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId member2 = new InstanceId( 2 );
        InstanceId member3 = new InstanceId( 3 );
        InstanceId member4 = new InstanceId( 4 );
        InstanceId member5 = new InstanceId( 5 );

        Timeouts timeouts = mock( Timeouts.class );

        CommonContextState commonState = mock( CommonContextState.class );
        ClusterConfiguration configuration = mock( ClusterConfiguration.class );
        when( commonState.configuration() ).thenReturn( configuration );
        when( configuration.getMembers() ).thenReturn( members( 5 ) );
        when( configuration.getMemberIds() ).thenReturn( ids( 5 ) );

        DelayedDirectExecutor executor = new DelayedDirectExecutor( NullLogProvider.getInstance() );
        HeartbeatContext context =
                new HeartbeatContextImpl( me, commonState, NullLogProvider.getInstance(), timeouts,
                        executor );

        final List<InstanceId> failed = new ArrayList<>( 4 );
        HeartbeatListener listener = new HeartbeatListener()
        {
            @Override
            public void failed( InstanceId server )
            {
                failed.add( server );
            }

            @Override
            public void alive( InstanceId server )
            {
                failed.remove( server );
            }
        };

        context.addHeartbeatListener( listener );

        // when
        // just two suspicions come, no extra failing action should be taken since this is not majority
        context.suspect( member2 );
        context.suspect( member3 );
        executor.drain();

        // then
        assertEquals( 0, failed.size() );

        // when
        // the another instance suspects them, therefore have a majority of non suspected, then 2 and 3 must fail
        Set<InstanceId> suspicionsFrom5 = new HashSet<>();
        suspicionsFrom5.add( member2 );
        suspicionsFrom5.add( member3 );
        context.suspicions( member5, suspicionsFrom5 );
        executor.drain();

        // then
        assertEquals( 2, failed.size() );
        assertTrue( failed.contains( member2 ) );
        assertTrue( failed.contains( member3 ) );

        // when
        // an instance sends a heartbeat, it should be set as alive
        context.alive( member2 );
        executor.drain();

        // then
        assertEquals( 1, failed.size() );
        assertTrue( failed.contains( member3 ) );
    }
}
