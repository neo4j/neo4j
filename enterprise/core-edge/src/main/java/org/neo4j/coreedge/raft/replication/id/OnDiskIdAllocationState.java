/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.replication.id;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.internal.DatabaseHealth;

import static org.neo4j.coreedge.raft.replication.id.InMemoryIdAllocationState.Serializer.NUMBER_OF_BYTES_PER_WRITE;

/**
 * The OnDiskAllocationState is a decorator around InMemoryIdAllocationState providing on-disk persistence of
 * InMemoryIdAllocationState instances. The purpose of this persistent state is to remember the cumulative effects
 * of RAFT-ed ID allocation such that the RAFT log can be safely truncated.
 * <p>
 * It is log structured for convenience and ease of operational problem solving.
 */
public class OnDiskIdAllocationState implements IdAllocationState
{
    private static final String FILENAME = "id.allocation.";
    private final InMemoryIdAllocationState inMemoryIdAllocationState;
    private final InMemoryIdAllocationState.Serializer serializer;

    private final int numberOfEntriesBeforeRotation;
    private final Supplier<DatabaseHealth> databaseHealthSupplier;

    private final File storeA;
    private final File storeB;

    private final FileSystemAbstraction fileSystemAbstraction;
    private File currentStoreFile;
    private final ByteBuffer buffer;
    private StoreChannel currentStoreChannel;

    public OnDiskIdAllocationState( FileSystemAbstraction fileSystemAbstraction, File storeDir,
                                    int numberOfEntriesBeforeRotation, Supplier<DatabaseHealth> databaseHealthSupplier )
            throws IOException
    {
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.numberOfEntriesBeforeRotation = numberOfEntriesBeforeRotation;
        this.databaseHealthSupplier = databaseHealthSupplier;

        this.serializer = new InMemoryIdAllocationState.Serializer();
        this.buffer = ByteBuffer.allocate( NUMBER_OF_BYTES_PER_WRITE );

        this.storeA = new File( storeDir, FILENAME + "A" );
        this.storeB = new File( storeDir, FILENAME + "B" );

        this.currentStoreFile = new IdAllocationStoreRecoveryManager( fileSystemAbstraction ).recover( storeA, storeB );
        currentStoreChannel = fileSystemAbstraction.open( currentStoreFile, "rw" );

        // we know that if the file is non empty, it is properly trimmed and should be read
        if ( fileSystemAbstraction.getFileSize( currentStoreFile ) > 0 )
        {
            currentStoreChannel.position( fileSystemAbstraction.getFileSize( currentStoreFile ) -
                    NUMBER_OF_BYTES_PER_WRITE );

            buffer.clear();
            currentStoreChannel.read( buffer );
            buffer.flip();
            inMemoryIdAllocationState = serializer.deserialize( buffer );
        }
        else
        {
            inMemoryIdAllocationState = new InMemoryIdAllocationState();
        }

    }

    @Override
    public int lastIdRangeLength( IdType idType )
    {
        return inMemoryIdAllocationState.lastIdRangeLength( idType );
    }

    @Override
    public void lastIdRangeLength( IdType idType, int idRangeLength )
    {
        inMemoryIdAllocationState.lastIdRangeLength( idType, idRangeLength );
    }

    @Override
    public long logIndex()
    {
        return inMemoryIdAllocationState.logIndex();
    }

    /**
     * This should be the last method called after updating the state. It has a
     * side-effect that it flushes the in-memory state to disk.
     *
     * @param logIndex The value to set as the last log index at which this state was updated
     */
    @Override
    public void logIndex( long logIndex )
    {
        inMemoryIdAllocationState.logIndex( logIndex );
        persistStoreData();
    }

    @Override
    public long firstUnallocated( IdType idType )
    {
        return inMemoryIdAllocationState.firstUnallocated( idType );
    }

    @Override
    public void firstUnallocated( IdType idType, long idRangeEnd )
    {
        inMemoryIdAllocationState.firstUnallocated( idType, idRangeEnd );
    }

    @Override
    public long lastIdRangeStart( IdType idType )
    {
        return inMemoryIdAllocationState.lastIdRangeStart( idType );
    }

    @Override
    public void lastIdRangeStart( IdType idType, long idRangeStart )
    {
        inMemoryIdAllocationState.lastIdRangeStart( idType, idRangeStart );
    }

    private synchronized void persistStoreData()
    {
        try
        {
            if ( fileSystemAbstraction.getFileSize( currentStoreFile ) >=
                    numberOfEntriesBeforeRotation *
                            NUMBER_OF_BYTES_PER_WRITE )
            {
                switchStoreFile();
            }

            buffer.clear();
            serializer.serialize( inMemoryIdAllocationState, buffer );
            buffer.flip();
            currentStoreChannel.writeAll( buffer );
            currentStoreChannel.force( false );
        }
        catch ( IOException e )
        {
            databaseHealthSupplier.get().panic( e );
        }
    }

    private void switchStoreFile() throws IOException
    {
        currentStoreChannel.close();

        if ( currentStoreFile.getName().toLowerCase().endsWith( "a" ) )
        {
            currentStoreChannel = initialiseStoreFile( storeB );
            currentStoreFile = storeB;
        }
        else if ( currentStoreFile.getName().toLowerCase().endsWith( "b" ) )
        {
            currentStoreChannel = initialiseStoreFile( storeA );
            currentStoreFile = storeA;
        }
    }

    private StoreChannel initialiseStoreFile( File nextStore ) throws IOException
    {
        if ( fileSystemAbstraction.fileExists( nextStore ) )
        {
            fileSystemAbstraction.deleteFile( nextStore );
        }
        return fileSystemAbstraction.create( nextStore );
    }

    public File currentStoreFile()
    {
        return currentStoreFile;
    }
}
