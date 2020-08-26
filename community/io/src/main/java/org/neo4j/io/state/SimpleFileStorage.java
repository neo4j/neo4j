/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.io.state;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.io.fs.PhysicalFlushableChannel;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.fs.ReadAheadChannel.DEFAULT_READ_AHEAD_SIZE;

public class SimpleFileStorage<T> implements SimpleStorage<T>
{
    private final FileSystemAbstraction fileSystem;
    private final ChannelMarshal<T> marshal;
    private final MemoryTracker memoryTracker;
    private final Path path;

    public SimpleFileStorage( FileSystemAbstraction fileSystem, Path path, ChannelMarshal<T> marshal, MemoryTracker memoryTracker )
    {
        this.fileSystem = fileSystem;
        this.path = path;
        this.marshal = marshal;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public boolean exists()
    {
        return fileSystem.fileExists( path );
    }

    @Override
    public T readState() throws IOException
    {
        try ( ReadableChannel channel = new ReadAheadChannel<>( fileSystem.read( path ),
                new NativeScopedBuffer( DEFAULT_READ_AHEAD_SIZE, memoryTracker ) ) )
        {
            return marshal.unmarshal( channel );
        }
        catch ( EndOfStreamException e )
        {
            throw new IOException( e );
        }
    }

    @Override
    public void writeState( T state ) throws IOException
    {
        if ( path.getParent() != null )
        {
            fileSystem.mkdirs( path.getParent() );
        }
        fileSystem.deleteFile( path );

        try ( FlushableChannel channel = new PhysicalFlushableChannel( fileSystem.write( path ),
                new NativeScopedBuffer( kibiBytes( 512 ), memoryTracker ) ) )
        {
            marshal.marshal( state, channel );
        }
    }

    @Override
    public void removeState() throws IOException
    {
        if ( exists() )
        {
            var deleted = fileSystem.deleteFile( path );
            if ( !deleted )
            {
                throw new IOException( String.format( "File %s could not be deleted", path.getFileName() ) );
            }
        }
    }
}
