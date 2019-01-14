/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api.impl.index.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.api.impl.index.storage.layout.FolderLayout;
import org.neo4j.string.UTF8;

/**
 * Helper class for storing a failure message that happens during an OutOfDisk situation in
 * a pre-allocated file
 */
public class FailureStorage
{
    private static final int MAX_FAILURE_SIZE = 16384;
    public static final String DEFAULT_FAILURE_FILE_NAME = "failure-message";

    private final FileSystemAbstraction fs;
    private final FolderLayout folderLayout;
    private final String failureFileName;

    /**
     * @param failureFileName name of failure files to be created
     * @param folderLayout describing where failure files should be stored
     */
    public FailureStorage( FileSystemAbstraction fs, FolderLayout folderLayout, String failureFileName )
    {
        this.fs = fs;
        this.folderLayout = folderLayout;
        this.failureFileName = failureFileName;
    }

    public FailureStorage( FileSystemAbstraction fs, FolderLayout folderLayout )
    {
        this( fs, folderLayout, DEFAULT_FAILURE_FILE_NAME );
    }

    /**
     * Create/reserve an empty failure file for the given indexId.
     *
     * This will overwrite any pre-existing failure file.
     *
     * @throws IOException if the failure file could not be created
     */
    public synchronized void reserveForIndex() throws IOException
    {
        fs.mkdirs( folderLayout.getIndexFolder() );
        File failureFile = failureFile();
        try ( StoreChannel channel = fs.create( failureFile ) )
        {
            channel.writeAll( ByteBuffer.wrap( new byte[MAX_FAILURE_SIZE] ) );
            channel.force( true );
        }
    }

    /**
     * Delete failure file for the given index id
     *
     */
    public synchronized void clearForIndex()
    {
        fs.deleteFile( failureFile() );
    }

    /**
     * @return the failure, if any. Otherwise {@code null} marking no failure.
     */
    public synchronized String loadIndexFailure()
    {
        File failureFile = failureFile();
        try
        {
            if ( !fs.fileExists( failureFile ) || !isFailed( failureFile ) )
            {
                return null;
            }
            return readFailure( failureFile );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    /**
     * Store failure in failure file for index with the given id
     *
     * @param failure message describing the failure that needs to be stored
     * @throws IOException if the failure could not be stored
     */
    public synchronized void storeIndexFailure( String failure ) throws IOException
    {
        File failureFile = failureFile();
        try ( StoreChannel channel = fs.open( failureFile, OpenMode.READ_WRITE ) )
        {
            byte[] existingData = new byte[(int) channel.size()];
            channel.readAll( ByteBuffer.wrap( existingData ) );
            channel.position( lengthOf( existingData ) );

            byte[] data = UTF8.encode( failure );
            channel.writeAll( ByteBuffer.wrap( data, 0, Math.min( data.length, MAX_FAILURE_SIZE ) ) );

            channel.force( true );
            channel.close();
        }
    }

    File failureFile()
    {
        File folder = folderLayout.getIndexFolder();
        return new File( folder, failureFileName );
    }

    private String readFailure( File failureFile ) throws IOException
    {
        try ( StoreChannel channel = fs.open( failureFile, OpenMode.READ ) )
        {
            byte[] data = new byte[(int) channel.size()];
            channel.readAll( ByteBuffer.wrap( data ) );
            return UTF8.decode( withoutZeros( data ) );
        }
    }

    private static byte[] withoutZeros( byte[] data )
    {
        byte[] result = new byte[ lengthOf(data) ];
        System.arraycopy( data, 0, result, 0, result.length );
        return result;
    }

    private static int lengthOf( byte[] data )
    {
        for ( int i = 0; i < data.length; i++ )
        {
            if ( 0 == data[i] )
            {
                return i;
            }
        }
        return data.length;
    }

    private boolean isFailed( File failureFile ) throws IOException
    {
        try ( StoreChannel channel = fs.open( failureFile, OpenMode.READ ) )
        {
            byte[] data = new byte[(int) channel.size()];
            channel.readAll( ByteBuffer.wrap( data ) );
            channel.close();
            return !allZero( data );
        }
    }

    private static boolean allZero( byte[] data )
    {
        for ( byte b : data )
        {
            if ( b != 0 )
            {
                return false;
            }
        }
        return true;
    }
}
