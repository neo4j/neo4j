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
package org.neo4j.coreedge.raft.state.term;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.state.StatePersister;
import org.neo4j.coreedge.raft.state.StateRecoveryManager;
import org.neo4j.coreedge.raft.state.term.InMemoryTermState.InMemoryTermStateChannelMarshal;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class OnDiskTermState extends LifecycleAdapter implements TermState
{
    public static final String FILENAME = "term.";
    public static final String DIRECTORY_NAME = "term-state";

    private InMemoryTermState inMemoryTermState;

    private final StatePersister<InMemoryTermState> statePersister;

    public OnDiskTermState( FileSystemAbstraction fileSystemAbstraction, File stateDir,
                            int numberOfEntriesBeforeRotation, Supplier<DatabaseHealth> databaseHealthSupplier )
            throws IOException
    {
        File fileA = new File( stateDir, FILENAME + "a" );
        File fileB = new File( stateDir, FILENAME + "b" );

        InMemoryTermStateChannelMarshal marshal = new InMemoryTermStateChannelMarshal();

        TermStateRecoveryManager recoveryManager = new TermStateRecoveryManager( fileSystemAbstraction, marshal );

        StateRecoveryManager.RecoveryStatus recoveryStatus = recoveryManager.recover( fileA, fileB );

        this.inMemoryTermState = recoveryManager.readLastEntryFrom( fileSystemAbstraction, recoveryStatus
                .previouslyActive() );

        this.statePersister = new StatePersister<>( fileA, fileB, fileSystemAbstraction, numberOfEntriesBeforeRotation,
                marshal, recoveryStatus.previouslyInactive(),
                databaseHealthSupplier );
    }

    @Override
    public void shutdown() throws Throwable
    {
        statePersister.close();
    }

    @Override
    public long currentTerm()
    {
        return inMemoryTermState.currentTerm();
    }

    @Override
    public void update( long newTerm ) throws RaftStorageException
    {
        inMemoryTermState.failIfInvalid( newTerm );
        try
        {
            if ( needsToWriteToDisk( newTerm ) )
            {
                InMemoryTermState tempState = new InMemoryTermState( inMemoryTermState );
                tempState.update( newTerm );
                statePersister.persistStoreData( tempState );
                inMemoryTermState = tempState;
            }
        }
        catch ( IOException e )
        {
            throw new RaftStorageException( e );
        }
    }

    private boolean needsToWriteToDisk( long newTerm )
    {
        return newTerm > inMemoryTermState.currentTerm();
    }
}
