/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.raft.membership;

import java.util.Set;
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.raft.state.membership.OnDiskRaftMembershipState;
import org.neo4j.coreedge.server.RaftTestMarshal;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class OnDiskRaftMembershipStateTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldRoundtripSingleMembershipSetState() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        fsa.mkdir( testDir.directory() );

        OnDiskRaftMembershipState<RaftTestMember> onDiskRaftMembershipState =
                new OnDiskRaftMembershipState<>( fsa, testDir.directory(), 100, mock( Supplier.class ),
                        new RaftTestMarshal(), NullLogProvider.getInstance() );

        final Set<RaftTestMember> members = new RaftTestGroup( 1, 2, 3, 4 ).getMembers();

        // when
        onDiskRaftMembershipState.setVotingMembers( members );

        // then
        assertEquals( new RaftTestGroup( 1, 2, 3, 4 ).getMembers(), onDiskRaftMembershipState.votingMembers() );
    }

    @Test
    public void shouldStoreCrashAndThenRetrieveSingleMembershipSet() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        fsa.mkdir( testDir.directory() );

        RaftTestGroup raftTestGroup = new RaftTestGroup( 1, 2, 3, 4 );
        long atLogIndex = 123L;

        OnDiskRaftMembershipState<RaftTestMember> initial = new
                OnDiskRaftMembershipState<>( fsa, testDir.directory(), 100, mock( Supplier.class ),
                new RaftTestMarshal(), NullLogProvider.getInstance() );
        initial.setVotingMembers( raftTestGroup.getMembers() );
        initial.logIndex( atLogIndex );

        // when
        final OnDiskRaftMembershipState<RaftTestMember> state =
                new OnDiskRaftMembershipState<>( fsa, testDir.directory(), 100, mock( Supplier.class ),
                        new RaftTestMarshal(), NullLogProvider.getInstance() );

        // then
        assertEquals( raftTestGroup.getMembers(), state.votingMembers() );
        assertEquals( atLogIndex, state.logIndex() );
    }

    @Test
    public void shouldProperlyRestoreAfterRotation() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        fsa.mkdir( testDir.directory() );

        RaftTestGroup raftTestGroup = new RaftTestGroup( 1, 2, 3, 4 );
        long startingLogIndex = 123L;

        int numberOfEntriesBeforeRotation = 5;
        OnDiskRaftMembershipState<RaftTestMember> state = new
                OnDiskRaftMembershipState<>( fsa, testDir.directory(), numberOfEntriesBeforeRotation,
                mock( Supplier.class ), new RaftTestMarshal(), NullLogProvider.getInstance() );

        // When
        // we write enough entries to cause rotation
        startingLogIndex = putSomeStateIn( raftTestGroup, startingLogIndex, numberOfEntriesBeforeRotation, state );

        // And we restore from that
        OnDiskRaftMembershipState<RaftTestMember> restored = new
                OnDiskRaftMembershipState<>( fsa, testDir.directory(), numberOfEntriesBeforeRotation,
                mock( Supplier.class ), new RaftTestMarshal(), NullLogProvider.getInstance() );

        // Then
        // The last entry should be what it was last written
        assertEquals( startingLogIndex, restored.logIndex() );
    }

    @Test
    public void shouldOnlyWriteToFileOnLogIndexUpdate() throws Exception
    {
        /*
         * This scenario is not possible during runtime, since we unconditionally write the log index after the
         * voting members and only an exception will stop it from happening. What this test verifies is that
         * a persist operation takes place only when the index is written and not before.
         */
        // given
        EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        fsa.mkdir( testDir.directory() );

        RaftTestGroup raftTestGroup = new RaftTestGroup( 1, 2, 3, 4 );

        OnDiskRaftMembershipState<RaftTestMember> state = new
                OnDiskRaftMembershipState<>( fsa, testDir.directory(), 10, mock( Supplier.class ),
                new RaftTestMarshal(), NullLogProvider.getInstance() );

        // when
        // we write one entry
        state.setVotingMembers( raftTestGroup.getMembers() );
        long atLogIndex = 123;
        state.logIndex( atLogIndex );
        // but the next does not include a log index update (this should never happen in reality)
        state.setVotingMembers( new RaftTestGroup( 6, 7 ).getMembers() );

        // Then
        // The last state read is the one that had the log index written
        OnDiskRaftMembershipState<RaftTestMember> restored = new
                OnDiskRaftMembershipState<>( fsa, testDir.directory(), 10, mock( Supplier.class ),
                new RaftTestMarshal(), NullLogProvider.getInstance() );
        assertEquals( raftTestGroup.getMembers(), restored.votingMembers() );
        assertEquals( atLogIndex, restored.logIndex() );
    }

    private long putSomeStateIn( RaftTestGroup raftTestGroup, long startingLogIndex, int
            numberOfEntriesBeforeRotation, OnDiskRaftMembershipState<RaftTestMember> state )
    {
        for ( int i = 0; i < numberOfEntriesBeforeRotation + 1; i++ )
        {
            state.setVotingMembers( raftTestGroup.getMembers() );
            state.logIndex( startingLogIndex++ );
        }
        return --startingLogIndex;
    }
}
