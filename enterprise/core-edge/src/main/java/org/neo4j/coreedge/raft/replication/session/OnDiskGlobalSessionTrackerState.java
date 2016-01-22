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
package org.neo4j.coreedge.raft.replication.session;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.coreedge.raft.replication.session.InMemoryGlobalSessionTrackerState
        .InMemoryGlobalSessionTrackerStateChannelMarshal;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.coreedge.raft.state.StatePersister;
import org.neo4j.coreedge.raft.state.StateRecoveryManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class OnDiskGlobalSessionTrackerState<MEMBER> extends LifecycleAdapter implements GlobalSessionTrackerState<MEMBER>
{
    public static final String DIRECTORY_NAME = "session-tracker-state";
    public static final String FILENAME = "session.tracker.";

    private InMemoryGlobalSessionTrackerState<MEMBER> inMemoryGlobalSessionTrackerState;

    private final StatePersister<InMemoryGlobalSessionTrackerState<MEMBER>> statePersister;

    public OnDiskGlobalSessionTrackerState( FileSystemAbstraction fileSystemAbstraction, File stateDir,
                                            ChannelMarshal<MEMBER> memberMarshal, int numberOfEntriesBeforeRotation,
                                            Supplier<DatabaseHealth> databaseHealthSupplier )
            throws IOException
    {
        InMemoryGlobalSessionTrackerStateChannelMarshal<MEMBER> marshal =
                new InMemoryGlobalSessionTrackerStateChannelMarshal<>( memberMarshal );

        File fileA = new File( stateDir, FILENAME + "a" );
        File fileB = new File( stateDir, FILENAME + "b" );

        GlobalSessionTrackerStateRecoveryManager<MEMBER> recoveryManager =
                new GlobalSessionTrackerStateRecoveryManager<>( fileSystemAbstraction, marshal );

        final StateRecoveryManager.RecoveryStatus recoveryStatus = recoveryManager.recover( fileA, fileB );

        this.inMemoryGlobalSessionTrackerState =
                recoveryManager.readLastEntryFrom( fileSystemAbstraction, recoveryStatus.previouslyActive() );

        this.statePersister = new StatePersister<>( fileA, fileB, fileSystemAbstraction, numberOfEntriesBeforeRotation,
                marshal, recoveryStatus.previouslyInactive(), databaseHealthSupplier );
    }

    @Override
    public boolean validateOperation( GlobalSession<MEMBER> globalSession, LocalOperationId
            localOperationId )
    {
        return inMemoryGlobalSessionTrackerState.validateOperation( globalSession, localOperationId );
    }

    @Override
    public void update( GlobalSession<MEMBER> globalSession, LocalOperationId localOperationId, long logIndex )
    {
        InMemoryGlobalSessionTrackerState<MEMBER> temp =
                new InMemoryGlobalSessionTrackerState<>( inMemoryGlobalSessionTrackerState );

        temp.update( globalSession, localOperationId, logIndex );

        try
        {
            statePersister.persistStoreData( temp );
            inMemoryGlobalSessionTrackerState = temp;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public long logIndex()
    {
        return inMemoryGlobalSessionTrackerState.logIndex();
    }

    @Override
    public void shutdown() throws Throwable
    {
        statePersister.close();
    }
}
