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
package org.neo4j.coreedge.raft.state.vote;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.state.StatePersister;
import org.neo4j.coreedge.raft.state.StateRecoveryManager;
import org.neo4j.coreedge.raft.state.membership.Marshal;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class OnDiskVoteState<MEMBER> extends LifecycleAdapter implements VoteState<MEMBER>
{
    public static final String FILENAME = "vote.";

    private final StatePersister<InMemoryVoteState<MEMBER>> statePersister;

    private InMemoryVoteState<MEMBER> inMemoryVoteState;

    public OnDiskVoteState( FileSystemAbstraction fileSystemAbstraction, File storeDir,
                            int numberOfEntriesBeforeRotation, Supplier<DatabaseHealth> databaseHealthSupplier,
                            Marshal<MEMBER> memberMarshal ) throws IOException
    {
        File fileA = new File( storeDir, FILENAME + "A" );
        File fileB = new File( storeDir, FILENAME + "B" );

        ByteBuffer workingBuffer = ByteBuffer.allocate( InMemoryVoteState.InMemoryVoteStateMarshal
                .NUMBER_OF_BYTES_PER_VOTE );

        InMemoryVoteState.InMemoryVoteStateMarshal<MEMBER> marshal = new InMemoryVoteState.InMemoryVoteStateMarshal<>( memberMarshal );

        VoteStateRecoveryManager<MEMBER> recoveryManager =
                new VoteStateRecoveryManager<>( fileSystemAbstraction, marshal );

        final StateRecoveryManager.RecoveryStatus recoveryStatus = recoveryManager.recover( fileA, fileB );


        this.inMemoryVoteState = recoveryManager.readLastEntryFrom( fileSystemAbstraction, recoveryStatus
                .previouslyActive() );


        this.statePersister = new StatePersister<>( fileA, fileB, fileSystemAbstraction, numberOfEntriesBeforeRotation,
                workingBuffer, marshal, recoveryStatus.previouslyInactive(), databaseHealthSupplier );
    }

    @Override
    public MEMBER votedFor()
    {
        return inMemoryVoteState.votedFor();
    }

    @Override
    public void votedFor( MEMBER votedFor, long term ) throws RaftStorageException
    {
        InMemoryVoteState<MEMBER> tempState = new InMemoryVoteState<>( inMemoryVoteState );
        tempState.votedFor( votedFor, term );

        try
        {
            statePersister.persistStoreData( tempState );
        }
        catch ( IOException e )
        {
            throw new RaftStorageException( e );
        }

        inMemoryVoteState = tempState;
    }

    @Override
    public long term()
    {
        return inMemoryVoteState.term();
    }

    @Override
    public void shutdown() throws Throwable
    {
        statePersister.close();
    }
}
