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
package org.neo4j.coreedge.raft.state;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class DurableStateStorage<STATE> extends LifecycleAdapter implements StateStorage<STATE>
{
    private STATE initialState;
    private final File fileA;
    private final File fileB;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final ChannelMarshal<STATE> marshal;
    private final Supplier<DatabaseHealth> databaseHealthSupplier;
    private final int numberOfEntriesBeforeRotation;

    private int numberOfEntriesWrittenInActiveFile;
    private File currentStoreFile;

    private PhysicalFlushableChannel currentStoreChannel;

    public DurableStateStorage( FileSystemAbstraction fileSystemAbstraction, File stateDir, String name,
                                StateMarshal<STATE> marshal, int numberOfEntriesBeforeRotation,
                                Supplier<DatabaseHealth> databaseHealthSupplier, LogProvider logProvider )
            throws IOException
    {
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.marshal = marshal;
        this.numberOfEntriesBeforeRotation = numberOfEntriesBeforeRotation;
        this.databaseHealthSupplier = databaseHealthSupplier;

        fileA = new File( stateDir, name + ".a" );
        fileB = new File( stateDir, name + ".b" );

        StateRecoveryManager<STATE> recoveryManager =
                new StateRecoveryManager<>( fileSystemAbstraction, marshal );

        final StateRecoveryManager.RecoveryStatus<STATE> recoveryStatus = recoveryManager.recover( fileA, fileB );

        this.currentStoreFile = recoveryStatus.activeFile();
        this.currentStoreChannel = new PhysicalFlushableChannel( fileSystemAbstraction.open( currentStoreFile, "rw" ) );
        initialiseStoreFile( currentStoreFile );

        this.initialState = recoveryStatus.recoveredState();

        Log log = logProvider.getLog( getClass() );
        log.info( "%s state restored, up to ordinal %d", name, marshal.ordinal( initialState ) );
    }

    @Override
    public STATE getInitialState()
    {
        return initialState;
    }

    @Override
    public synchronized void shutdown() throws Throwable
    {
        currentStoreChannel.close();
        currentStoreChannel = null;
    }

    @Override
    public synchronized void persistStoreData( STATE state ) throws IOException
    {
        try
        {
            if ( numberOfEntriesWrittenInActiveFile >= numberOfEntriesBeforeRotation )
            {
                switchStoreFile();
                numberOfEntriesWrittenInActiveFile = 0;
            }

            marshal.marshal( state, currentStoreChannel );
            currentStoreChannel.prepareForFlush().flush();

            numberOfEntriesWrittenInActiveFile++;
        }
        catch ( IOException e )
        {
            databaseHealthSupplier.get().panic( e );
            throw e;
        }
    }

    private void switchStoreFile() throws IOException
    {
        currentStoreChannel.close();

        if ( currentStoreFile.getName().toLowerCase().endsWith( "a" ) )
        {
            currentStoreChannel = initialiseStoreFile( fileB );
            currentStoreFile = fileB;
        }
        else if ( currentStoreFile.getName().toLowerCase().endsWith( "b" ) )
        {
            currentStoreChannel = initialiseStoreFile( fileA );
            currentStoreFile = fileA;
        }
    }

    private PhysicalFlushableChannel initialiseStoreFile( File nextStore ) throws IOException
    {
        if ( fileSystemAbstraction.fileExists( nextStore ) )
        {
            fileSystemAbstraction.truncate( nextStore, 0 );
            return new PhysicalFlushableChannel( fileSystemAbstraction.open( nextStore, "rw" ) );
        }
        else
        {
            return new PhysicalFlushableChannel( fileSystemAbstraction.create( nextStore ) );
        }
    }
}
