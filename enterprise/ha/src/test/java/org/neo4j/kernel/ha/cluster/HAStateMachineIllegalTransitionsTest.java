/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.ha.cluster;

import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.kernel.impl.store.StoreId;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachineTest.mockAddClusterMemberListener;
import static org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher.SLAVE;

/*
 * These tests reproduce state transitions which are illegal. The general requirement for them is that they
 * set the instance to PENDING state and ask for an election, in the hopes that the result will come with
 * proper ordering and therefore cause a proper state transition chain to MASTER or SLAVE.
 */
public class HAStateMachineIllegalTransitionsTest
{
    private final InstanceId me = new InstanceId( 1 );
    private ClusterMemberListener memberListener;
    private HighAvailabilityMemberStateMachine stateMachine;
    private Election election;

    @Before
    public void setup()
    {
        HighAvailabilityMemberContext context = new SimpleHighAvailabilityMemberContext( me, false );

        ClusterMemberEvents events = mock( ClusterMemberEvents.class );
        HighAvailabilityMemberStateMachineTest.ClusterMemberListenerContainer memberListenerContainer =
                mockAddClusterMemberListener( events );

        election = mock( Election.class );

        stateMachine = buildMockedStateMachine( context, events, election );
        stateMachine.init();
        memberListener = memberListenerContainer.get();
        HighAvailabilityMemberStateMachineTest.HAStateChangeListener probe = new
                HighAvailabilityMemberStateMachineTest.HAStateChangeListener();
        stateMachine.addHighAvailabilityMemberListener( probe );
    }

    @Test
    public void shouldProperlyHandleMasterAvailableWhenInPending()
    {
        /*
         * If the instance is in PENDING state, masterIsAvailable for itself should leave it to PENDING
         * and ask for elections
         */

        // sanity check of starting state
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );

        // when
        // It receives available master without having gone through TO_MASTER
        memberListener.memberIsAvailable( MASTER, me, URI.create( "ha://whatever" ), StoreId.DEFAULT );

        // then
        assertPendingStateAndElectionsAsked();
    }

    @Test
    public void shouldProperlyHandleSlaveAvailableWhenInPending()
    {
        /*
         * If the instance is in PENDING state, slaveIsAvailable for itself should set it to PENDING
         * and ask for elections
         */
        // sanity check of starting state
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );

        // when
        // It receives available SLAVE without having gone through TO_SLAVE
        memberListener.memberIsAvailable( SLAVE, me, URI.create( "ha://whatever" ), StoreId.DEFAULT );

        // then
        assertPendingStateAndElectionsAsked();
    }

    @Test
    public void shouldProperlyHandleNonElectedMasterBecomingAvailableWhenInToSlave()
    {
        /*
         * If the instance is in TO_SLAVE and a masterIsAvailable comes that does not refer to the elected master,
         * the instance should go to PENDING and ask for elections
         */
        // Given
        InstanceId other = new InstanceId( 2 );
        InstanceId rogueMaster = new InstanceId( 3 );

        // sanity check of starting state
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );

        // when
        // It becomes available master without having gone through TO_MASTER
        memberListener.memberIsAvailable( MASTER, other, URI.create( "ha://whatever" ), StoreId.DEFAULT );

        // sanity check it is TO_SLAVE
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.TO_SLAVE ) );

        // when
        // it receives that another master became master but which is different than the currently elected one
        memberListener.memberIsAvailable( MASTER, rogueMaster, URI.create( "ha://fromNowhere" ), StoreId.DEFAULT );

        // then
        assertPendingStateAndElectionsAsked();
    }

    @Test
    public void shouldProperlyHandleConflictingMasterAvailableMessage()
    {
        /*
         * If the instance is currently in TO_MASTER and a masterIsAvailable comes for another instance, then
         * this instance should transition to PENDING and ask for an election.
         */
        // Given
        InstanceId rogue = new InstanceId( 2 );
        // sanity check of starting state
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );

        // when
        // It receives available master without having gone through TO_MASTER
        memberListener.coordinatorIsElected( me );

        // then
        // sanity check it transitioned to TO_MASTER
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.TO_MASTER ) );

        // when
        // it receives a masterIsAvailable for another instance
        memberListener.memberIsAvailable( MASTER, rogue, URI.create( "ha://someUri" ), StoreId.DEFAULT );

        // then
        assertPendingStateAndElectionsAsked();
    }

    @Test
    public void shouldProperlyHandleConflictingSlaveIsAvailableMessageWhenInToMaster()
    {
        /*
         * If the instance is in TO_MASTER state, slaveIsAvailable for itself should set it to PENDING
         * and ask for elections
         */
        // Given
        // sanity check of starting state
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );

        // when
        // It receives available master without having gone through TO_MASTER
        memberListener.coordinatorIsElected( me );

        // then
        // sanity check it transitioned to TO_MASTER
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.TO_MASTER ) );

        // when
        // it receives a masterIsAvailable for another instance
        memberListener.memberIsAvailable( SLAVE, me, URI.create( "ha://someUri" ), StoreId.DEFAULT );

        // then
        assertPendingStateAndElectionsAsked();
    }

    @Test
    public void shouldProperlyHandleConflictingSlaveIsAvailableWhenInMaster()
    {
        /*
         * If the instance is in MASTER state, slaveIsAvailable for itself should set it to PENDING
         * and ask for elections
         */
        // sanity check of starting state
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );

        // when
        // It receives available master without having gone through TO_MASTER
        memberListener.coordinatorIsElected( me );

        // then
        // sanity check it transitioned to TO_MASTER
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.TO_MASTER ) );

        // when
        // it receives a masterIsAvailable for itself, completing the transition
        memberListener.memberIsAvailable( MASTER, me, URI.create( "ha://someUri" ), StoreId.DEFAULT );

        // then
        // it should move to MASTER
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.MASTER ) );

        // when
        // it receives a slaveIsAvailable for itself while in the MASTER state
        memberListener.memberIsAvailable( SLAVE, me, URI.create( "ha://someUri" ), StoreId.DEFAULT );

        // then
        assertPendingStateAndElectionsAsked();
    }

    @Test
    public void shouldProperlyHandleMasterIsAvailableWhenInMasterState()
    {
        /*
         * If the instance is in MASTER state and a masterIsAvailable is received for another instance, then
         * this instance should got to PENDING and ask for elections
         */
        // Given
        InstanceId rogue = new InstanceId( 2 );
        // sanity check of starting state
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );

        // when
        // It receives available master without having gone through TO_MASTER
        memberListener.coordinatorIsElected( me );

        // then
        // sanity check it transitioned to TO_MASTER
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.TO_MASTER ) );

        // when
        // it receives a masterIsAvailable for itself, completing the transition
        memberListener.memberIsAvailable( MASTER, me, URI.create( "ha://someUri" ), StoreId.DEFAULT );

        // then
        // it should move to MASTER
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.MASTER ) );

        // when
        // it receives a slaveIsAvailable for itself while in the MASTER state
        memberListener.memberIsAvailable( MASTER, rogue, URI.create( "ha://someUri" ), StoreId.DEFAULT );

        // then
        assertPendingStateAndElectionsAsked();
    }

    @Test
    public void shouldProperlyHandleMasterIsAvailableWhenInSlaveState()
    {
        /*
         * If the instance is in SLAVE state and receives masterIsAvailable for an instance different than the
         * current master, it should revert to PENDING and ask for elections
         */
        // Given
        InstanceId master = new InstanceId( 2 );
        InstanceId rogueMaster = new InstanceId( 3 );
        // sanity check of starting state
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );

        // when
        // a master is elected normally
        memberListener.coordinatorIsElected( master );
        memberListener.memberIsAvailable( MASTER, master, URI.create( "ha://someUri" ), StoreId.DEFAULT );

        // then
        // sanity check it transitioned to TO_SLAVE
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.TO_SLAVE ) );

        // when
        memberListener.memberIsAvailable( SLAVE, me, URI.create( "ha://myUri" ), StoreId.DEFAULT );

        // then
        // we should be in SLAVE state
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.SLAVE ) );

        // when
        // it receives a masterIsAvailable for an unelected master while in the slave state
        memberListener.memberIsAvailable( MASTER, rogueMaster, URI.create( "ha://someOtherUri" ), StoreId.DEFAULT );

        // then
        assertPendingStateAndElectionsAsked();
    }

    private void assertPendingStateAndElectionsAsked()
    {
        // it should remain in pending
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
        // and it should ask for elections
        verify( election ).performRoleElections();
    }

    private HighAvailabilityMemberStateMachine buildMockedStateMachine( HighAvailabilityMemberContext context,
                                                                        ClusterMemberEvents events,
                                                                        Election election )
    {
        return new HighAvailabilityMemberStateMachineTest.StateMachineBuilder()
                .withContext( context ).withEvents( events ).withElection( election ).build();
    }
}
