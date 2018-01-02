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
package org.neo4j.kernel.ha.cluster;

import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import org.neo4j.cluster.InstanceId;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.ILLEGAL;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.MASTER;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.PENDING;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.SLAVE;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.TO_MASTER;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.TO_SLAVE;

/**
 * This is the full specification for state switching in HA according to incoming cluster
 * messages. 5 states times 3 possible messages makes for 15 methods - each comes with 2 or 3 cases for the
 * message and context contents.
 *
 * No behaviour is examined here - interactions with context and actions taken are not tested. See other tests for that.
 */
public class HighAvailabilityMemberStateTest
{
    public static final URI SampleUri = URI.create( "ha://foo" );
    private InstanceId myId;
    private HighAvailabilityMemberContext context;

    @Before
    public void setup()
    {
        myId = new InstanceId( 1 );
        context = mock( HighAvailabilityMemberContext.class );
        when( context.getMyId() ).thenReturn( myId );
    }

    @Test
    public void testPendingMasterIsElected()
    {
        // CASE 1: Got MasterIsElected for me - should switch to TO_MASTER
        HighAvailabilityMemberState newState = PENDING.masterIsElected( context, myId );
        assertEquals( TO_MASTER, newState );

        // CASE 2: Got MasterIsElected for someone else - should remain to PENDING
        HighAvailabilityMemberState newStateCase2 = PENDING.masterIsElected( context, new InstanceId( 2 ) );
        assertEquals( PENDING, newStateCase2 );
    }

    @Test
    public void testPendingMasterIsAvailable()
    {
        // CASE 1: Got MasterIsAvailable for me - should not happen
        HighAvailabilityMemberState illegal = PENDING.masterIsAvailable( context, myId, SampleUri );
        assertEquals( ILLEGAL, illegal );

        // CASE 2: Got MasterIsAvailable for someone else - should transition to TO_SLAVE
        // TODO test correct info is passed through to context
        HighAvailabilityMemberState newState = PENDING.masterIsAvailable( context, new InstanceId( 2 ), SampleUri );
        assertEquals( TO_SLAVE, newState );
    }

    @Test
    public void testPendingSlaveIsAvailable()
    {
        // CASE 1: Got SlaveIsAvailable for me - should not happen, that's what TO_SLAVE exists for
        HighAvailabilityMemberState illegal = PENDING.slaveIsAvailable( context, myId, SampleUri );
        assertEquals( ILLEGAL, illegal );

        // CASE 2: Got SlaveIsAvailable for someone else - it's ok, remain in PENDING
        HighAvailabilityMemberState newState = PENDING.slaveIsAvailable( context, new InstanceId( 2 ), SampleUri );
        assertEquals( PENDING, newState );
    }

    @Test
    public void testToMasterMasterIsElected()
    {
        // CASE 1: Got MasterIsElected for me - it's ok, continue in TO_MASTER
        HighAvailabilityMemberState newState = TO_MASTER.masterIsElected( context, myId );
        assertEquals( TO_MASTER, newState );

        // CASE 2: Got MasterIsElected for someone else - switch to PENDING
        HighAvailabilityMemberState newStateCase2 = TO_MASTER.masterIsElected( context, new InstanceId( 2 ) );
        assertEquals( PENDING, newStateCase2 );
    }

    @Test
    public void testToMasterMasterIsAvailable()
    {
        // CASE 1: Got MasterIsAvailable for me - it's ok, that means we completed switching and should to to MASTER
        HighAvailabilityMemberState newState = TO_MASTER.masterIsAvailable( context, myId, SampleUri );
        assertEquals( MASTER, newState );

        // CASE 2: Got MasterIsAvailable for someone else - should not happen, should have received a MasterIsElected
        HighAvailabilityMemberState illegal = TO_MASTER.masterIsAvailable( context, new
                InstanceId( 2 ), SampleUri );
        assertEquals( ILLEGAL, illegal );
    }

    @Test
    public void testToMasterSlaveIsAvailable()
    {
        // CASE 1: Got SlaveIsAvailable for me - not ok, i'm currently switching to master
        HighAvailabilityMemberState illegal = TO_MASTER.slaveIsAvailable( context, myId,
                SampleUri );
        assertEquals( ILLEGAL, illegal );

        // CASE 2: Got SlaveIsAvailable for someone else - don't really care
        HighAvailabilityMemberState newState = TO_MASTER.slaveIsAvailable( context, new InstanceId( 2 ), SampleUri );
        assertEquals( TO_MASTER, newState );
    }

    @Test
    public void testMasterMasterIsElected()
    {
        // CASE 1: Got MasterIsElected for me. Should remain master.
        HighAvailabilityMemberState newState = MASTER.masterIsElected( context, myId );
        assertEquals( MASTER, newState );

        // CASE 2: Got MasterIsElected for someone else. Should switch to pending.
        HighAvailabilityMemberState newStateCase2 = MASTER.masterIsElected( context, new InstanceId( 2 ) );
        assertEquals( PENDING, newStateCase2 );
    }

    @Test
    public void testMasterMasterIsAvailable()
    {
        // CASE 1: Got MasterIsAvailable for someone else - should fail.
        HighAvailabilityMemberState illegal = MASTER.masterIsAvailable( context, new InstanceId(
                2 ), SampleUri );
        assertEquals( ILLEGAL, illegal );

        // CASE 2: Got MasterIsAvailable for us - it's ok, should pass
        HighAvailabilityMemberState newState = MASTER.masterIsAvailable( context, myId, SampleUri );
        assertEquals( MASTER, newState );
    }

    @Test
    public void testMasterSlaveIsAvailable()
    {
        // CASE 1: Got SlaveIsAvailable for me - should fail.
        HighAvailabilityMemberState illegal = MASTER.slaveIsAvailable( context, myId, SampleUri );
        assertEquals( ILLEGAL, illegal );

        // CASE 2: Got SlaveIsAvailable for someone else - who cares? Should succeed.
        HighAvailabilityMemberState newState = MASTER.slaveIsAvailable( context, new InstanceId( 2 ), SampleUri );
        assertEquals( MASTER, newState );
    }

    @Test
    public void testToSlaveMasterIsElected()
    {
        // CASE 1: Got MasterIsElected for me - should switch to TO_MASTER
        HighAvailabilityMemberState newState = TO_SLAVE.masterIsElected( context, myId );
        assertEquals( TO_MASTER, newState );

        // CASE 2: Got MasterIsElected for someone else - should switch to PENDING
        HighAvailabilityMemberState newStateCase2 = TO_SLAVE.masterIsElected( context, new InstanceId( 2 ) );
        assertEquals( PENDING, newStateCase2 );
    }

    @Test
    public void testToSlaveMasterIsAvailable()
    {
        // CASE 1: Got MasterIsAvailable for me - should fail, i am currently trying to become slave
        HighAvailabilityMemberState illegal = TO_SLAVE.masterIsAvailable( context, myId,
                SampleUri );
        assertEquals( ILLEGAL, illegal );


        // CASE 2: Got MasterIsAvailable for someone else who is already the master - should continue switching
        InstanceId currentMaster = new InstanceId( 2 );
        when( context.getElectedMasterId() ).thenReturn( currentMaster );
        HighAvailabilityMemberState newState = TO_SLAVE.masterIsAvailable( context, currentMaster, SampleUri );
        assertEquals( TO_SLAVE, newState );

        // CASE 3: Got MasterIsAvailable for someone else who is not the master - should fail
        HighAvailabilityMemberState moreIllegal = TO_SLAVE.masterIsAvailable( context, new InstanceId
                ( 3 ), SampleUri );
        assertEquals( ILLEGAL, moreIllegal );
    }

    @Test
    public void testToSlaveSlaveIsAvailable()
    {
        // CASE 1: It is me that that is available as slave - cool, go to SLAVE
        HighAvailabilityMemberState newState = TO_SLAVE.slaveIsAvailable( context, myId, SampleUri );
        assertEquals( SLAVE, newState );

        // CASE 2: It is someone else that completed the switch - ignore, continue
        HighAvailabilityMemberState newStateCase2 = TO_SLAVE.slaveIsAvailable( context, new InstanceId( 2 ), SampleUri );

        assertEquals( TO_SLAVE, newStateCase2 );
    }

    @Test
    public void testSlaveMasterIsElected()
    {
        // CASE 1: It is me that got elected master - should switch to TO_MASTER
        HighAvailabilityMemberState newState = SLAVE.masterIsElected( context, myId );
        assertEquals( TO_MASTER, newState );

        InstanceId masterInstanceId = new InstanceId( 2 );
        when( context.getElectedMasterId() ).thenReturn( masterInstanceId );
        // CASE 2: It is someone else that got elected master - should switch to PENDING
        HighAvailabilityMemberState newStateCase2 = SLAVE.masterIsElected( context, new InstanceId( 3 ) );
        assertEquals( PENDING, newStateCase2 );

        // CASE 3: It is the current master that got elected again - ignore
        HighAvailabilityMemberState newStateCase3 = SLAVE.masterIsElected( context, masterInstanceId );
        assertEquals( SLAVE, newStateCase3 );
    }

    @Test
    public void testSlaveMasterIsAvailable()
    {
        // CASE 1: It is me who is available as master - i don't think so
        HighAvailabilityMemberState illegal = SLAVE.masterIsAvailable( context, myId, SampleUri );
        assertEquals( ILLEGAL, illegal );

        // CASE 2: It is someone else that is available as master and is not the master now - missed the election, fail
        InstanceId masterInstanceId = new InstanceId( 2 );
        when( context.getElectedMasterId() ).thenReturn( masterInstanceId );
        HighAvailabilityMemberState moreIllegal = SLAVE.masterIsAvailable( context, new InstanceId( 3
        ), SampleUri );
        assertEquals( ILLEGAL, moreIllegal );

        // CASE 3: It is the same master as now - it's ok, stay calm and carry on
        HighAvailabilityMemberState newState = SLAVE.masterIsAvailable( context, masterInstanceId, SampleUri );
        assertEquals( SLAVE, newState );
    }

    @Test
    public void testSlaveSlaveIsAvailable()
    {
        // CASE 1 and only - always remain in SLAVE
        assertEquals( SLAVE, SLAVE.slaveIsAvailable( mock( HighAvailabilityMemberContext.class ), mock( InstanceId.class), SampleUri ) );
    }
}
