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
package org.neo4j.test.ha;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.net.URI;
import java.util.Objects;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.paxos.MemberIsAvailable;
import org.neo4j.cluster.member.paxos.PaxosClusterMemberEvents;
import org.neo4j.cluster.member.paxos.PaxosClusterMemberEvents.ClusterMembersSnapshot;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha.cluster.HANewSnapshotFunction;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher.SLAVE;
import static org.neo4j.kernel.impl.store.StoreId.DEFAULT;

public class ClusterMembersSnapshotTest
{
    private static final String URI = "http://me";

    @Test
    public void snapshotListPrunesSameMemberOnIdenticalAvailabilityEvents() throws Exception
    {
        // GIVEN
        // -- a snapshot containing one member with a role
        ClusterMembersSnapshot snapshot = new ClusterMembersSnapshot(
                new PaxosClusterMemberEvents.UniqueRoleFilter()
        );
        URI clusterUri = new URI( URI );
        InstanceId instanceId = new InstanceId( 1 );
        MemberIsAvailable memberIsAvailable =
                new MemberIsAvailable( MASTER, instanceId, clusterUri, new URI( URI + "?something" ), DEFAULT );
        snapshot.availableMember( memberIsAvailable );

        // WHEN
        // -- the same member and role gets added to the snapshot
        snapshot.availableMember( memberIsAvailable );

        // THEN
        // -- getting the snapshot list should only reveal the last one
        assertEquals( 1, Iterables.count( snapshot.getCurrentAvailable( instanceId ) ) );
        assertThat( snapshot.getCurrentAvailable( instanceId ), hasItem( memberIsAvailable( memberIsAvailable ) ) );
        assertEquals( 1, Iterables.count( snapshot.getCurrentAvailableMembers() ) );
        assertThat( snapshot.getCurrentAvailableMembers(), hasItems( memberIsAvailable( memberIsAvailable ) ) );
    }

    @Test
    public void snapshotListShouldContainOnlyOneEventForARoleWithTheSameIdWhenSwitchingFromMasterToSlave()
            throws Exception
    {
        // GIVEN
        // -- a snapshot containing one member with a role
        ClusterMembersSnapshot snapshot = new ClusterMembersSnapshot( new HANewSnapshotFunction() );
        URI clusterUri = new URI( URI );
        InstanceId instanceId = new InstanceId( 1 );
        MemberIsAvailable event1 = new MemberIsAvailable(
                MASTER, instanceId, clusterUri, new URI( URI + "?something" ), DEFAULT );
        snapshot.availableMember( event1 );

        // WHEN
        // -- the same member, although different role, gets added to the snapshot
        MemberIsAvailable event2 = new MemberIsAvailable(
                SLAVE, instanceId, clusterUri, new URI( URI + "?something" ), DEFAULT );
        snapshot.availableMember( event2 );

        // THEN
        // -- getting the snapshot list should reveal both
        assertEquals( 1, Iterables.count( snapshot.getCurrentAvailable( instanceId ) ) );
        assertThat( snapshot.getCurrentAvailable( instanceId ), hasItems( memberIsAvailable( event2 ) ) );
        assertEquals( 1, Iterables.count( snapshot.getCurrentAvailableMembers() ) );
        assertThat( snapshot.getCurrentAvailableMembers(), hasItems( memberIsAvailable( event2 ) ) );
    }

    @Test
    public void snapshotListShouldContainOnlyOneEventForARoleWithTheSameIdWhenSwitchingFromSlaveToMaster()
            throws Exception
    {
        // GIVEN
        // -- a snapshot containing one member with a role
        ClusterMembersSnapshot snapshot = new ClusterMembersSnapshot( new HANewSnapshotFunction() );
        URI clusterUri = new URI( URI );
        InstanceId instanceId = new InstanceId( 1 );
        MemberIsAvailable event1 = new MemberIsAvailable( SLAVE, instanceId, clusterUri,
                new URI( URI + "?something" ), DEFAULT );
        snapshot.availableMember( event1 );

        // WHEN
        // -- the same member, although different role, gets added to the snapshot
        MemberIsAvailable event2 = new MemberIsAvailable( MASTER, instanceId, clusterUri,
                new URI( URI + "?something" ), DEFAULT );
        snapshot.availableMember( event2 );

        // THEN
        // -- getting the snapshot list should reveal both
        assertEquals( 1, Iterables.count( snapshot.getCurrentAvailable( instanceId ) ) );
        assertThat( snapshot.getCurrentAvailable( instanceId ), hasItems( memberIsAvailable( event2 ) ) );
        assertEquals( 1, Iterables.count( snapshot.getCurrentAvailableMembers() ) );
        assertThat( snapshot.getCurrentAvailableMembers(), hasItems( memberIsAvailable( event2 ) ) );
    }

    @Test
    public void snapshotListPrunesOtherMemberWithSameMasterRole() throws Exception
    {
        // GIVEN
        // -- a snapshot containing one member with a role
        ClusterMembersSnapshot snapshot = new ClusterMembersSnapshot( new HANewSnapshotFunction() );
        URI clusterUri = new URI( URI );
        InstanceId instanceId = new InstanceId( 1 );
        MemberIsAvailable event = new MemberIsAvailable(
                MASTER, instanceId, clusterUri, new URI( URI + "?something1" ), DEFAULT );
        snapshot.availableMember( event );

        // WHEN
        // -- another member, but with same role, gets added to the snapshot
        URI otherClusterUri = new URI( URI );
        InstanceId otherInstanceId = new InstanceId( 2 );
        MemberIsAvailable otherEvent = new MemberIsAvailable(
                MASTER, otherInstanceId, otherClusterUri, new URI( URI + "?something2" ), DEFAULT );
        snapshot.availableMember( otherEvent );

        // THEN
        // -- getting the snapshot list should only reveal the last member added, as it had the same role
        assertEquals( 1, Iterables.count( snapshot.getCurrentAvailable( otherInstanceId ) ) );
        assertThat( snapshot.getCurrentAvailable( otherInstanceId ), hasItems( memberIsAvailable( otherEvent ) ) );
        assertEquals( 1, Iterables.count( snapshot.getCurrentAvailableMembers() ) );
        assertThat( snapshot.getCurrentAvailableMembers(), hasItems( memberIsAvailable( otherEvent ) ) );
    }

    @Test
    public void snapshotListDoesNotPruneOtherMemberWithSlaveRole() throws Exception
    {
        // GIVEN
        // -- a snapshot containing one member with a role
        ClusterMembersSnapshot snapshot = new ClusterMembersSnapshot( new HANewSnapshotFunction() );
        URI clusterUri = new URI( URI );
        InstanceId instanceId = new InstanceId( 1 );
        MemberIsAvailable event = new MemberIsAvailable( SLAVE, instanceId, clusterUri,
                new URI( URI + "?something1" ), DEFAULT );
        snapshot.availableMember( event );

        // WHEN
        // -- another member, but with same role, gets added to the snapshot
        URI otherClusterUri = new URI( URI );
        InstanceId otherInstanceId = new InstanceId( 2 );
        MemberIsAvailable otherEvent = new MemberIsAvailable( SLAVE, otherInstanceId, otherClusterUri,
                new URI( URI + "?something2" ), DEFAULT );
        snapshot.availableMember( otherEvent );

        // THEN
        assertEquals( 2, Iterables.count( snapshot.getCurrentAvailableMembers() ) );
        assertThat( snapshot.getCurrentAvailableMembers(),
                hasItems( memberIsAvailable( event ), memberIsAvailable( otherEvent ) ) );
    }

    private static Matcher<MemberIsAvailable> memberIsAvailable( final MemberIsAvailable expected )
    {
        return new BaseMatcher<MemberIsAvailable>()
        {
            @Override
            public boolean matches( Object item )
            {
                MemberIsAvailable input = (MemberIsAvailable) item;
                return Objects.equals( input.getClusterUri(), expected.getClusterUri() ) &&
                        Objects.equals( input.getRole(), expected.getRole() ) &&
                        Objects.equals( input.getRoleUri(), expected.getRoleUri() );
            }

            @Override
            public void describeTo( Description description )
            {
            }
        };
    }
}
