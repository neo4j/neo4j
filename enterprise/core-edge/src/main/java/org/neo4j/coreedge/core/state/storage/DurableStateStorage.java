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
package org.neo4j.coreedge.core.state.storage;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.coreedge.core.state.StateRecoveryManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class DurableStateStorage<STATE> extends LifecycleAdapter implements StateStorage<STATE>
{
    private final StateRecoveryManager<STATE> recoveryManager;
    private final Log log;
    private final boolean mustExist;
    private STATE initialState;
    private final File fileA;
    private final File fileB;
    private final FileSystemAbstraction fsa;
    private final String name;
    private final StateMarshal<STATE> marshal;
    private final Supplier<DatabaseHealth> databaseHealthSupplier;
    private final int numberOfEntriesBeforeRotation;

    private int numberOfEntriesWrittenInActiveFile;
    private File currentStoreFile;

    private PhysicalFlushableChannel currentStoreChannel;

    static File stateDir( File baseDir, String name )
    {
        return new File( baseDir, name + "-state" );
    }

    public DurableStateStorage( FileSystemAbstraction fsa, File baseDir, String name,
            StateMarshal<STATE> marshal, int numberOfEntriesBeforeRotation,
            Supplier<DatabaseHealth> databaseHealthSupplier, LogProvider logProvider, boolean mustExist )
    {
        this.fsa = fsa;
        this.name = name;
        this.marshal = marshal;
        this.numberOfEntriesBeforeRotation = numberOfEntriesBeforeRotation;
        this.databaseHealthSupplier = databaseHealthSupplier;
        this.log = logProvider.getLog( getClass() );
        this.mustExist = mustExist;
        this.recoveryManager = new StateRecoveryManager<>( fsa, marshal );
        this.fileA = new File( stateDir( baseDir, name ), name + ".a" );
        this.fileB = new File( stateDir( baseDir, name ), name + ".b" );
    }

    private void create() throws IOException
    {
        ensureExists( fileA );
        ensureExists( fileB );
    }

    private void ensureExists( File file ) throws IOException
    {
        if ( !fsa.fileExists( file ) )
        {
            if ( mustExist )
            {
                throw new IllegalStateException( "File was expected to exist" );
            }

            fsa.mkdirs( file.getParentFile() );
            try ( FlushableChannel channel = new PhysicalFlushableChannel( fsa.create( file ) ) )
            {
                marshal.marshal( marshal.startState(), channel );
            }
        }
    }

    private void recover() throws IOException
    {
        final StateRecoveryManager.RecoveryStatus<STATE> recoveryStatus = recoveryManager.recover( fileA, fileB );

        this.currentStoreFile = recoveryStatus.activeFile();
        this.currentStoreChannel = resetStoreFile( currentStoreFile );
        this.initialState = recoveryStatus.recoveredState();

        log.info( "%s state restored, up to ordinal %d", name, marshal.ordinal( initialState ) );
    }

    @Override
    public STATE getInitialState()
    {
        assert initialState != null;
        return initialState;
    }

    @Override
    public void init() throws IOException
    {
        create();
        recover();
    }

    @Override
    public synchronized void shutdown() throws IOException
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

    void switchStoreFile() throws IOException
    {
        currentStoreChannel.close();

        if ( currentStoreFile.equals( fileA ) )
        {
            currentStoreChannel = resetStoreFile( fileB );
            currentStoreFile = fileB;
        }
        else
        {
            currentStoreChannel = resetStoreFile( fileA );
            currentStoreFile = fileA;
        }
    }

    private PhysicalFlushableChannel resetStoreFile( File nextStore ) throws IOException
    {
        fsa.truncate( nextStore, 0 );
        return new PhysicalFlushableChannel( fsa.open( nextStore, "rw" ) );
    }
}
