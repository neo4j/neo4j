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
import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.coreedge.raft.state.StatePersister;
import org.neo4j.coreedge.raft.state.StateRecoveryManager;
import org.neo4j.coreedge.raft.state.membership.InMemoryRaftMembershipState.InMemoryRaftMembershipStateChannelMarshal;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class OnDiskRaftMembershipState<MEMBER> extends LifecycleAdapter implements RaftMembershipState<MEMBER>
{
    private static final String FILENAME = "membership.state.";
    public static final String DIRECTORY_NAME = "membership-state";

    private final StatePersister<InMemoryRaftMembershipState<MEMBER>> statePersister;


    private InMemoryRaftMembershipState<MEMBER> inMemoryRaftMembershipState;

    public OnDiskRaftMembershipState( FileSystemAbstraction fileSystemAbstraction,
                                      File stateDir,
                                      int numberOfEntriesBeforeRotation,
                                      Supplier<DatabaseHealth> databaseHealthSupplier,
                                      ChannelMarshal<MEMBER> channelMarshal ) throws IOException
    {

        InMemoryRaftMembershipStateChannelMarshal<MEMBER> marshal =
                new InMemoryRaftMembershipStateChannelMarshal<>( channelMarshal );

        File fileA = new File( stateDir, FILENAME + "a" );
        File fileB = new File( stateDir, FILENAME + "b" );

        RaftMembershipStateRecoveryManager<MEMBER> recoveryManager = new RaftMembershipStateRecoveryManager<>(
                fileSystemAbstraction, marshal );

        final StateRecoveryManager.RecoveryStatus recoveryStatus = recoveryManager.recover( fileA, fileB );

        inMemoryRaftMembershipState = recoveryManager.readLastEntryFrom( recoveryStatus.previouslyActive() );

        this.statePersister = new StatePersister<>( fileA, fileB, fileSystemAbstraction, numberOfEntriesBeforeRotation,
                marshal,
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
        InMemoryRaftMembershipState<MEMBER> tempState = new InMemoryRaftMembershipState<>( inMemoryRaftMembershipState );
        tempState.logIndex( index );

        inMemoryRaftMembershipState.logIndex( index );

        try
        {
            statePersister.persistStoreData( inMemoryRaftMembershipState );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
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

    @Override
    public void shutdown() throws Throwable
    {
        statePersister.close();
    }
}
