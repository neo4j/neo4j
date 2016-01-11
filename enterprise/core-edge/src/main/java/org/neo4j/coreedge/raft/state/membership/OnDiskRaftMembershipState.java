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
package org.neo4j.coreedge.raft.state.membership;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.coreedge.raft.state.StatePersister;
import org.neo4j.coreedge.raft.state.StateRecoveryManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.DatabaseHealth;

public class OnDiskRaftMembershipState<MEMBER> implements RaftMembershipState<MEMBER>
{
    public static final int MAX_SIZE_OF_ADDRESS_STATE_ON_DISK = 2_000_000;
    private static final String FILENAME = "membership.state.";

    private final StatePersister<InMemoryRaftMembershipState<MEMBER>> statePersister;


    private InMemoryRaftMembershipState<MEMBER> inMemoryRaftMembershipState;

    public OnDiskRaftMembershipState( FileSystemAbstraction fileSystemAbstraction,
                                      File storeDir,
                                      int numberOfEntriesBeforeRotation,
                                      Supplier<DatabaseHealth> databaseHealthSupplier,
                                      Marshal<MEMBER> memberMarshal ) throws IOException
    {
        InMemoryRaftMembershipState.InMemoryRaftMembershipStateMarshal<MEMBER> marshal = new InMemoryRaftMembershipState.InMemoryRaftMembershipStateMarshal<>(
                memberMarshal );

        File fileA = new File( storeDir, FILENAME + "A" );
        File fileB = new File( storeDir, FILENAME + "B" );

        RaftMembershipStateRecoveryManager<MEMBER> recoveryManager = new RaftMembershipStateRecoveryManager<>(
                fileSystemAbstraction, marshal );

        final StateRecoveryManager.RecoveryStatus recoveryStatus = recoveryManager.recover( fileA, fileB );

        inMemoryRaftMembershipState = recoveryManager.readLastEntryFrom( recoveryStatus.previouslyActive() );

        this.statePersister = new StatePersister<>( fileA, fileB, fileSystemAbstraction, numberOfEntriesBeforeRotation,
                ByteBuffer.allocate( MAX_SIZE_OF_ADDRESS_STATE_ON_DISK ), marshal,
                recoveryStatus.previouslyInactive(),
                databaseHealthSupplier );
    }

    @Override
    public void setVotingMembers( Set<MEMBER> members )
    {
        inMemoryRaftMembershipState.setVotingMembers( members );
    }

    @Override
    public void addAdditionalReplicationMember( MEMBER catchingUpMember )
    {
        inMemoryRaftMembershipState.addAdditionalReplicationMember( catchingUpMember );
    }

    @Override
    public void removeAdditionalReplicationMember( MEMBER catchingUpMember )
    {
        inMemoryRaftMembershipState.removeAdditionalReplicationMember( catchingUpMember );
    }

    @Override
    public void logIndex( long index )
    {
        inMemoryRaftMembershipState.logIndex( index );
        statePersister.persistStoreData( inMemoryRaftMembershipState );
    }

    @Override
    public Set<MEMBER> votingMembers()
    {
        return inMemoryRaftMembershipState.votingMembers();
    }

    @Override
    public Set<MEMBER> replicationMembers()
    {
        return inMemoryRaftMembershipState.replicationMembers();
    }

    @Override
    public long logIndex()
    {
        return inMemoryRaftMembershipState.logIndex();
    }

    @Override
    public void registerListener( Listener listener )
    {
        inMemoryRaftMembershipState.registerListener( listener );
    }

    @Override
    public void deregisterListener( Listener listener )
    {
        inMemoryRaftMembershipState.deregisterListener( listener );
    }

}
