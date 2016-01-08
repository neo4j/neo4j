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
import java.nio.ByteBuffer;
import java.util.function.Supplier;

import org.neo4j.coreedge.raft.state.membership.Marshal;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.internal.DatabaseHealth;

public class StatePersister<STATE>
{
    private final File fileA;
    private final File fileB;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final Marshal<STATE> marshal;
    private final ByteBuffer buffer;
    private final Supplier<DatabaseHealth> databaseHealthSupplier;
    private final int numberOfEntriesBeforeRotation;

    private int numberOfEntriesWrittenInActiveFile;
    private File currentStoreFile;
    private StoreChannel currentStoreChannel;

    public StatePersister( File fileA, File fileB, FileSystemAbstraction fileSystemAbstraction,
                           int numberOfEntriesBeforeRotation, ByteBuffer buffer,
                           Marshal<STATE> marshal,
                           File currentStoreFile, Supplier<DatabaseHealth> databaseHealthSupplier )
            throws IOException
    {
        this.fileA = fileA;
        this.fileB = fileB;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.numberOfEntriesBeforeRotation = numberOfEntriesBeforeRotation;
        this.buffer = buffer;
        this.marshal = marshal;
        this.currentStoreFile = currentStoreFile;
        this.currentStoreChannel = fileSystemAbstraction.open( currentStoreFile, "rw" );
        this.databaseHealthSupplier = databaseHealthSupplier;
    }

    public synchronized void persistStoreData( STATE state )
    {
        try
        {
            if ( numberOfEntriesWrittenInActiveFile >= numberOfEntriesBeforeRotation )
            {
                switchStoreFile();
                numberOfEntriesWrittenInActiveFile = 0;
            }

            buffer.clear();
            marshal.marshal( state, buffer );
            buffer.flip();
            currentStoreChannel.writeAll( buffer );
            currentStoreChannel.force( false );
            numberOfEntriesWrittenInActiveFile++;
        }
        catch ( IOException e )
        {
            databaseHealthSupplier.get().panic( e );
        }
    }

    public void switchStoreFile() throws IOException
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

    public StoreChannel initialiseStoreFile( File nextStore ) throws IOException
    {
        if ( fileSystemAbstraction.fileExists( nextStore ) )
        {
            fileSystemAbstraction.deleteFile( nextStore );
        }
        return fileSystemAbstraction.create( nextStore );
    }
}
