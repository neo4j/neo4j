/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.index.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

/**
 * Helper class for storing a failure message that happens during an OutOfDisk situation in
 * a pre-allocated file
 */
public class FailureStorage
{
    public static final int MAX_FAILURE_SIZE = 16384;
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
     * @param indexId id of the index
     * @throws IOException if the failure file could not be created
     */
    public synchronized void reserveForIndex( long indexId ) throws IOException
    {
        fs.mkdirs( folderLayout.getFolder( indexId ) );
        File failureFile = failureFile( indexId );
        try ( StoreChannel channel = fs.create( failureFile ) )
        {
            channel.write( ByteBuffer.wrap( new byte[MAX_FAILURE_SIZE] ) );
            channel.force( true );
            channel.close();
        }
    }

    /**
     * Delete failure file for the given index id
     *
     * @param indexId of the index that failed
     */
    public synchronized void clearForIndex( long indexId )
    {
        fs.deleteFile( failureFile( indexId ) );
    }

    /**
     * @return the failure, if any. Otherwise {@code null} marking no failure.
     */
    public synchronized String loadIndexFailure( long indexId )
    {
        File failureFile = failureFile( indexId );
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
     * @param indexId of the index that failed
     * @param failure message describing the failure that needs to be stored
     * @throws IOException if the failure could not be stored
     */
    public synchronized void storeIndexFailure( long indexId, String failure ) throws IOException
    {
        File failureFile = failureFile( indexId );
        try ( StoreChannel channel = fs.open( failureFile, "rw" ) )
        {
            byte[] data = failure.getBytes( "utf-8" );
            channel.write( ByteBuffer.wrap( data, 0, Math.min( data.length, MAX_FAILURE_SIZE ) ) );

            channel.force( true );
            channel.close();
        }
    }

    File failureFile( long indexId )
    {
        File folder = folderLayout.getFolder( indexId );
        return new File( folder, failureFileName );
    }

    private String readFailure( File failureFile ) throws IOException
    {
        try ( StoreChannel channel = fs.open( failureFile, "r" ) )
        {
            byte[] data = new byte[(int) channel.size()];
            int readData = channel.read( ByteBuffer.wrap( data ) );
            channel.close();

            return readData <= 0 ? "" : new String( withoutZeros( data ), "utf-8" );
        }
    }

    private byte[] withoutZeros( byte[] data )
    {
        byte[] result = new byte[ lengthOf(data) ];
        System.arraycopy( data, 0, result, 0, result.length );
        return result;
    }

    private int lengthOf( byte[] data )
    {
        for (int i = 0; i < data.length; i++ )
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
        try ( StoreChannel channel = fs.open( failureFile, "r" ) )
        {
            byte[] data = new byte[(int) channel.size()];
            channel.read( ByteBuffer.wrap( data ) );
            channel.close();
            return !allZero( data );
        }
    }

    private boolean allZero( byte[] data )
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
