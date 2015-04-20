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
package org.neo4j.kernel.ha.cluster.member;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.heartbeat.Heartbeat;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.FakeClock;
import org.neo4j.kernel.ha.cluster.member.ClusterMemberVersionCheck.Outcome;
import org.neo4j.kernel.impl.store.StoreId;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.SLAVE;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.UNKNOWN;

public class ClusterMemberVersionCheckTest
{
    private static final StoreId EXPECTED_STORE_ID = new StoreId( 1, 2, 3, 4 );
    private static final StoreId UNEXPECTED_STORE_ID = new StoreId( 5, 6, 7, 8 );

    private static final InstanceId ID_1 = new InstanceId( 1 );
    private static final InstanceId ID_2 = new InstanceId( 2 );
    private static final InstanceId ID_3 = new InstanceId( 3 );
    private static final InstanceId ID_4 = new InstanceId( 4 );
    private static final InstanceId ID_5 = new InstanceId( 5 );

    @Test
    public void shouldReturnTrueIfAllAreAvailableAndNoMismatchesFound() throws InterruptedException
    {
        // Given
        ClusterMembers members = clusterMembersWith(
                member( ID_1, MASTER, true, EXPECTED_STORE_ID, true ),
                member( ID_3, SLAVE, true, EXPECTED_STORE_ID, true ),
                member( ID_4, SLAVE, true, EXPECTED_STORE_ID, true ),
                member( ID_5, SLAVE, true, EXPECTED_STORE_ID, true )
        );

        // When
        Outcome outcome = new ClusterMemberVersionCheck( members, ID_2, new FakeClock() )
                .doVersionCheck( EXPECTED_STORE_ID, 1, SECONDS );

        // Then
        assertFalse( outcome.hasMismatched() );
        assertFalse( outcome.hasUnavailable() );
        verify( members, times( 0 ) ).waitForEvent( anyLong() );
    }

    @Test
    public void shouldReturnTrueIfAllAreAvailableAndNoMismatchesFoundWithOldVersion() throws InterruptedException
    {
        // Given
        ClusterMembers members = clusterMembersWith(
                member( ID_1, MASTER, true, StoreId.DEFAULT, true ),
                member( ID_3, SLAVE, true, EXPECTED_STORE_ID, true ),
                member( ID_4, SLAVE, true, StoreId.DEFAULT, true ),
                member( ID_5, SLAVE, true, EXPECTED_STORE_ID, true )
        );

        // When
        Outcome outcome = new ClusterMemberVersionCheck( members, ID_2, new FakeClock() )
                .doVersionCheck( EXPECTED_STORE_ID, 1, SECONDS );

        // Then
        assertFalse( outcome.hasMismatched() );
        assertFalse( outcome.hasUnavailable() );
        verify( members, times( 0 ) ).waitForEvent( anyLong() );
    }

    @Test
    public void shouldReturnTrueIfAllAreAvailableAndHaveOldVersions() throws InterruptedException
    {
        // Given
        ClusterMembers members = clusterMembersWith(
                member( ID_1, MASTER, true, StoreId.DEFAULT, true ),
                member( ID_2, SLAVE, true, StoreId.DEFAULT, true ),
                member( ID_3, SLAVE, true, StoreId.DEFAULT, true ),
                member( ID_4, SLAVE, true, StoreId.DEFAULT, true )
        );

        // When
        Outcome outcome = new ClusterMemberVersionCheck( members, ID_5, new FakeClock() )
                .doVersionCheck( EXPECTED_STORE_ID, 1, SECONDS );

        // Then
        assertFalse( outcome.hasMismatched() );
        assertFalse( outcome.hasUnavailable() );
        verify( members, times( 0 ) ).waitForEvent( anyLong() );
    }

    @Test
    public void shouldReturnFalseIfAllAreAvailableAndMismatchesFound() throws InterruptedException
    {
        // Given
        ClusterMembers members = clusterMembersWith(
                member( ID_1, MASTER, true, EXPECTED_STORE_ID, true ),
                member( ID_3, SLAVE, true, EXPECTED_STORE_ID, true ),
                member( ID_4, SLAVE, true, UNEXPECTED_STORE_ID, true ),
                member( ID_5, SLAVE, true, EXPECTED_STORE_ID, true )
        );

        // When
        Outcome outcome = new ClusterMemberVersionCheck( members, ID_2, new FakeClock() )
                .doVersionCheck( EXPECTED_STORE_ID, 1, SECONDS );

        // Then
        assertTrue( outcome.hasMismatched() );
        assertThat( outcome.getMismatched(), equalTo( singletonMap( ID_4.toIntegerIndex(), UNEXPECTED_STORE_ID ) ) );
        assertFalse( outcome.hasUnavailable() );
        verify( members, times( 0 ) ).waitForEvent( anyLong() );
    }

    @Test
    public void shouldReturnFalseIfNotAllAreAvailableAndNoMismatchesFound() throws InterruptedException
    {
        // Given
        ClusterMembers members = clusterMembersWith(
                member( ID_1, MASTER, true, EXPECTED_STORE_ID, true ),
                member( ID_3, SLAVE, true, EXPECTED_STORE_ID, true ),
                member( ID_4, SLAVE, true, EXPECTED_STORE_ID, true ),
                member( ID_5, UNKNOWN, true, EXPECTED_STORE_ID, true )
        );

        // When
        int timeoutMillis = 1;
        Outcome outcome = new ClusterMemberVersionCheck( members, ID_2, twoTickClock( timeoutMillis ) )
                .doVersionCheck( EXPECTED_STORE_ID, timeoutMillis, MILLISECONDS );

        // Then
        assertFalse( outcome.hasMismatched() );
        assertTrue( outcome.hasUnavailable() );
        assertThat( outcome.getUnavailable(), equalTo( singleton( ID_5.toIntegerIndex() ) ) );
        verify( members, times( 1 ) ).waitForEvent( timeoutMillis );
    }

    @Test
    public void shouldReturnTrueIfAllEventuallyBecomeAvailableAndNoMismatchesFound() throws InterruptedException
    {
        // Given
        ClusterMembers members = changingClusterMembers(
                asList( member( ID_1, MASTER, true, EXPECTED_STORE_ID, true ),
                        member( ID_3, SLAVE, true, EXPECTED_STORE_ID, true ),
                        member( ID_4, SLAVE, true, EXPECTED_STORE_ID, true ),
                        member( ID_5, UNKNOWN, true, EXPECTED_STORE_ID, true ) ),
                member( ID_5, SLAVE, true, EXPECTED_STORE_ID, true ) );

        // When
        int timeoutMillis = 1;
        Outcome outcome = new ClusterMemberVersionCheck( members, ID_2, twoTickClock( timeoutMillis ) )
                .doVersionCheck( EXPECTED_STORE_ID, timeoutMillis, MILLISECONDS );

        // Then
        assertFalse( outcome.hasMismatched() );
        assertFalse( outcome.hasUnavailable() );
        verify( members, times( 1 ) ).waitForEvent( timeoutMillis );
    }

    @Test
    public void shouldReturnFalseIfNotAllAreAvailableAndMismatchesFound() throws InterruptedException
    {
        // Given
        ClusterMembers members = clusterMembersWith(
                member( ID_1, MASTER, true, EXPECTED_STORE_ID, true ),
                member( ID_3, SLAVE, true, EXPECTED_STORE_ID, true ),
                member( ID_4, SLAVE, true, UNEXPECTED_STORE_ID, true ),
                member( ID_5, UNKNOWN, true, EXPECTED_STORE_ID, true )
        );

        // When
        int timeoutMillis = 1;
        Outcome outcome = new ClusterMemberVersionCheck( members, ID_2, twoTickClock( timeoutMillis ) )
                .doVersionCheck( EXPECTED_STORE_ID, timeoutMillis, MILLISECONDS );

        // Then
        assertTrue( outcome.hasMismatched() );
        assertThat( outcome.getMismatched(), equalTo( singletonMap( ID_4.toIntegerIndex(), UNEXPECTED_STORE_ID ) ) );
        assertTrue( outcome.hasUnavailable() );
        assertThat( outcome.getUnavailable(), equalTo( singleton( ID_5.toIntegerIndex() ) ) );
        verify( members, times( 1 ) ).waitForEvent( timeoutMillis );
    }

    @Test(timeout = 5_000)
    public void shouldBeNotifiedAboutAvailableMemberWithExpectedStoreIdByListener() throws Exception
    {
        testNotificationAboutSlave5BeingUpWith( EXPECTED_STORE_ID );
    }

    @Test(timeout = 5_000)
    public void shouldBeNotifiedAboutAvailableMemberWithUnexpectedStoreIdByListener() throws Exception
    {
        testNotificationAboutSlave5BeingUpWith( UNEXPECTED_STORE_ID );
    }

    private void testNotificationAboutSlave5BeingUpWith( StoreId storeId ) throws Exception
    {
        // Given
        ClusterMemberListener[] memberListenerSlot = new ClusterMemberListener[1];
        ClusterMemberEvents clusterMemberEvents = clusterMemberEvents( memberListenerSlot );

        ClusterListener[] clusterListenerSlot = new ClusterListener[1];
        Cluster cluster = cluster( clusterListenerSlot );

        final ClusterMembers members =
                new ClusterMembers( cluster, mock( Heartbeat.class ), clusterMemberEvents, mock( InstanceId.class ) );

        ClusterMemberListener memberListener = memberListenerSlot[0];
        ClusterListener clusterListener = clusterListenerSlot[0];

        clusterListener.enteredCluster( clusterConfigurationWith( ID_1, ID_2, ID_3, ID_4, ID_5 ) );

        memberListener.memberIsAvailable( MASTER, ID_1, URI.create( "cluster://master" ), EXPECTED_STORE_ID );
        memberListener.memberIsAvailable( SLAVE, ID_3, URI.create( "cluster://slave3" ), EXPECTED_STORE_ID );
        memberListener.memberIsAvailable( SLAVE, ID_4, URI.create( "cluster://slave4" ), EXPECTED_STORE_ID );
        memberListener.memberIsUnavailable( SLAVE, ID_5 );

        // When
        final int timeoutMillis = 30_000;
        Future<Outcome> outcomeFuture = Executors.newSingleThreadExecutor().submit( new Callable<Outcome>()
        {
            @Override
            public Outcome call() throws Exception
            {
                return new ClusterMemberVersionCheck( members, ID_2, Clock.SYSTEM_CLOCK )
                        .doVersionCheck( EXPECTED_STORE_ID, timeoutMillis, MILLISECONDS );
            }
        } );

        Thread.sleep( 2_000 );

        memberListener.memberIsAvailable( SLAVE, ID_5, URI.create( "cluster://slave5" ), storeId );

        // Then
        Outcome outcome = outcomeFuture.get();
        assertFalse( outcome.hasUnavailable() );

        if ( storeId == EXPECTED_STORE_ID )
        {
            assertFalse( outcome.hasMismatched() );
        }
        else
        {
            assertTrue( outcome.hasMismatched() );
            assertThat(
                    outcome.getMismatched(),
                    equalTo( singletonMap( ID_5.toIntegerIndex(), UNEXPECTED_STORE_ID ) ) );
        }
    }

    private static ClusterMembers clusterMembersWith( ClusterMember... members )
    {
        ClusterMembers clusterMembers = mock( ClusterMembers.class );
        when( clusterMembers.getMembers() ).thenReturn( asList( members ) );
        return clusterMembers;
    }

    private static ClusterMembers changingClusterMembers( List<ClusterMember> initialMembers, ClusterMember lastMember )
    {
        ClusterMembers clusterMembers = mock( ClusterMembers.class );
        List<ClusterMember> finalMembers = new ArrayList<>( initialMembers );
        finalMembers.set( finalMembers.size() - 1, lastMember );
        when( clusterMembers.getMembers() ).thenReturn( initialMembers ).thenReturn( finalMembers );
        return clusterMembers;
    }

    private static ClusterMember member( InstanceId id, String role, boolean alive, StoreId storeId, boolean isInitial )
    {
        Map<String, URI> haRoles = singletonMap( role, URI.create( "cluster://test" + id ) );
        return new ClusterMember( id, haRoles, storeId, alive, isInitial );
    }

    private static Clock twoTickClock( long timeout )
    {
        return when( mock( Clock.class ).currentTimeMillis() ).thenReturn( 0L ).thenReturn( timeout + 1 ).getMock();
    }

    private static ClusterMemberEvents clusterMemberEvents( final ClusterMemberListener[] listenerSlot )
    {
        ClusterMemberEvents clusterMemberEvents = mock( ClusterMemberEvents.class );

        doAnswer( new Answer<Void>()
        {
            @Override
            public Void answer( InvocationOnMock invocation ) throws Throwable
            {
                listenerSlot[0] = ((ClusterMemberListener) invocation.getArguments()[0]);
                return null;
            }
        } ).when( clusterMemberEvents ).addClusterMemberListener( any( ClusterMemberListener.class ) );

        return clusterMemberEvents;
    }

    private static Cluster cluster( final ClusterListener[] listenerSlot )
    {
        Cluster cluster = mock( Cluster.class );

        doAnswer( new Answer<Void>()
        {
            @Override
            public Void answer( InvocationOnMock invocation ) throws Throwable
            {
                listenerSlot[0] = ((ClusterListener) invocation.getArguments()[0]);
                return null;
            }
        } ).when( cluster ).addClusterListener( any( ClusterListener.class ) );

        return cluster;
    }

    private static ClusterConfiguration clusterConfigurationWith( InstanceId... memberIds )
    {
        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        when( clusterConfiguration.getMemberIds() ).thenReturn( Arrays.asList( memberIds ) );
        return clusterConfiguration;
    }
}
