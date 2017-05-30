/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.store.id;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.InvalidIdGeneratorException;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;


public class IdFile
{
    public static final long NO_RESULT = -1;

    // sticky(byte), nextFreeId(long)
    public static final int HEADER_SIZE = Byte.BYTES + Long.BYTES;

    // if sticky the id generator wasn't closed properly so it has to be
    // rebuilt (go through the node, relationship, property, rel type etc files)
    private static final byte CLEAN_GENERATOR = (byte) 0;
    private static final byte STICKY_GENERATOR = (byte) 1;

    private final File file;
    private final FileSystemAbstraction fs;
    private StoreChannel fileChannel;

    private FreeIdKeeper freeIdKeeper;

    private final int grabSize;
    private final boolean aggressiveReuse;

    private long initialHighId;

    private boolean closed = true;

    public IdFile( FileSystemAbstraction fs, File file, int grabSize, boolean aggressiveReuse )
    {
        if ( grabSize < 1 )
        {
            throw new IllegalArgumentException( "Illegal grabSize: " + grabSize );
        }

        this.file = file;
        this.fs = fs;
        this.grabSize = grabSize;
        this.aggressiveReuse = aggressiveReuse;
    }

    // initialize the id generator and performs a simple validation
    void init()
    {
        try
        {
            fileChannel = fs.open( file, "rw" );
            initialHighId = readAndValidateHeader();
            markAsSticky();

            this.freeIdKeeper = new FreeIdKeeper( fileChannel, grabSize, aggressiveReuse, HEADER_SIZE );
            closed = false;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                    "Unable to init id generator " + file, e );
        }
    }

    public boolean isClosed()
    {
        return closed;
    }

    public long getInitialHighId()
    {
        return initialHighId;
    }

    void assertStillOpen()
    {
        if ( closed )
        {
            throw new IllegalStateException( "Closed id generator " + file );
        }
    }

    private long readAndValidateHeader() throws IOException
    {
        try
        {
            return readAndValidate( fileChannel, file );
        }
        catch ( InvalidIdGeneratorException e )
        {
            fileChannel.close();
            throw e;
        }
    }

    private static long readAndValidate( StoreChannel channel, File fileName ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( HEADER_SIZE );
        int read = channel.read( buffer );
        if ( read != HEADER_SIZE )
        {
            throw new InvalidIdGeneratorException(
                    "Unable to read header, bytes read: " + read );
        }
        buffer.flip();
        byte storageStatus = buffer.get();
        if ( storageStatus != CLEAN_GENERATOR )
        {
            throw new InvalidIdGeneratorException( "Sticky generator[ " +
                    fileName + "] delete this id file and build a new one" );
        }
        return buffer.getLong();
    }

    public static long readHighId( FileSystemAbstraction fileSystem, File file ) throws IOException
    {
        try ( StoreChannel channel = fileSystem.open( file, "r" ) )
        {
            return readAndValidate( channel, file );
        }
    }

    private void markAsSticky() throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( Byte.BYTES );
        buffer.put( STICKY_GENERATOR ).flip();
        fileChannel.position( 0 );
        fileChannel.write( buffer );
        fileChannel.force( false );
    }

    private void markAsCleanlyClosed(  ) throws IOException
    {
        // remove sticky
        ByteBuffer buffer = ByteBuffer.allocate( Byte.BYTES );
        buffer.put( CLEAN_GENERATOR ).flip();
        fileChannel.position( 0 );
        fileChannel.write( buffer );
    }

    public void close( long highId )
    {
        if ( closed )
        {
            return;
        }

        try
        {
            freeIdKeeper.close(); // first write out free ids, then mark as clean
            writeHeader( highId );
            fileChannel.force( false );

            markAsCleanlyClosed( );

            closeChannel();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                    "Unable to close id generator " + file, e );
        }
    }

    private void closeChannel() throws IOException
    {
        // flush and close
        fileChannel.force( false );
        fileChannel.close();
        fileChannel = null;
        closed = true;
    }

    private void writeHeader( long highId ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( HEADER_SIZE );
        buffer.put( STICKY_GENERATOR ).putLong( highId ).flip();
        fileChannel.position( 0 );
        fileChannel.write( buffer );
    }

    public void delete()
    {
        if ( !closed )
        {
            try
            {
                closeChannel();
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( "Unable to safe close id generator " + file, e );
            }
        }

        if ( !fs.deleteFile( file ) )
        {
            throw new UnderlyingStorageException( "Unable to delete id generator " + file );
        }
    }

    /**
     *
     * @return -1 if no availabele
     */
    public long getReuseableId()
    {
        return freeIdKeeper.getId();
    }

    public void freeId( long id )
    {
        freeIdKeeper.freeId( id );
    }

    public long getFreeIdCount()
    {
        return freeIdKeeper.getCount();
    }

    /**
     * Creates a new id generator.
     *
     * @param fileName The name of the id generator
     * @param throwIfFileExists if {@code true} will cause an {@link UnderlyingStorageException} to be thrown if
     * the file already exists. if {@code false} will truncate the file writing the header in it.
     */
    public static void createEmptyIdFile( FileSystemAbstraction fs, File fileName, long highId,
            boolean throwIfFileExists )
    {
        // sanity checks
        if ( fs == null )
        {
            throw new IllegalArgumentException( "Null filesystem" );
        }
        if ( fileName == null )
        {
            throw new IllegalArgumentException( "Null filename" );
        }
        if ( throwIfFileExists && fs.fileExists( fileName ) )
        {
            throw new IllegalStateException( "Can't create IdGeneratorFile["
                    + fileName + "], file already exists" );
        }
        try ( StoreChannel channel = fs.create( fileName ) )
        {
            // write the header
            channel.truncate( 0 );
            ByteBuffer buffer = ByteBuffer.allocate( HEADER_SIZE );
            buffer.put( CLEAN_GENERATOR ).putLong( highId ).flip();
            channel.write( buffer );
            channel.force( false );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                    "Unable to create id generator" + fileName, e );
        }
    }

    @Override
    public String toString()
    {
        return "IdFile{" + "file=" + file + ", fs=" + fs + ", fileChannel=" + fileChannel + ", defragCount=" +
                freeIdKeeper.getCount() + ", grabSize=" + grabSize + ", aggressiveReuse=" +
                aggressiveReuse + ", closed=" + closed + '}';
    }
}
