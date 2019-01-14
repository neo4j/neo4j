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
package org.neo4j.causalclustering.core.state.storage;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.core.state.StateRecoveryManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class DurableStateStorage<STATE> extends LifecycleAdapter implements StateStorage<STATE>
{
    private final StateRecoveryManager<STATE> recoveryManager;
    private final Log log;
    private STATE initialState;
    private final File fileA;
    private final File fileB;
    private final FileSystemAbstraction fsa;
    private final String name;
    private final StateMarshal<STATE> marshal;
    private final int numberOfEntriesBeforeRotation;

    private int numberOfEntriesWrittenInActiveFile;
    private File currentStoreFile;

    private PhysicalFlushableChannel currentStoreChannel;

    public DurableStateStorage( FileSystemAbstraction fsa, File baseDir, String name, StateMarshal<STATE> marshal,
            int numberOfEntriesBeforeRotation, LogProvider logProvider )
    {
        this.fsa = fsa;
        this.name = name;
        this.marshal = marshal;
        this.numberOfEntriesBeforeRotation = numberOfEntriesBeforeRotation;
        this.log = logProvider.getLog( getClass() );
        this.recoveryManager = new StateRecoveryManager<>( fsa, marshal );
        File parent = stateDir( baseDir, name );
        this.fileA = new File( parent, name + ".a" );
        this.fileB = new File( parent, name + ".b" );
    }

    public boolean exists()
    {
        return fsa.fileExists( fileA ) && fsa.fileExists( fileB );
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
        if ( numberOfEntriesWrittenInActiveFile >= numberOfEntriesBeforeRotation )
        {
            switchStoreFile();
            numberOfEntriesWrittenInActiveFile = 0;
        }

        marshal.marshal( state, currentStoreChannel );
        currentStoreChannel.prepareForFlush().flush();

        numberOfEntriesWrittenInActiveFile++;
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
        return new PhysicalFlushableChannel( fsa.open( nextStore, OpenMode.READ_WRITE ) );
    }

    static File stateDir( File baseDir, String name )
    {
        return new File( baseDir, name + "-state" );
    }
}
