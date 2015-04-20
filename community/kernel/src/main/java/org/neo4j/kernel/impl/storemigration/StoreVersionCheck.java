/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.helpers.UTF8;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

import static org.neo4j.kernel.impl.storemigration.StoreVersionCheck.Result.Outcome;

public class StoreVersionCheck
{
    private final FileSystemAbstraction fs;

    public StoreVersionCheck( FileSystemAbstraction fs )
    {
        this.fs = fs;
    }

    public Result hasVersion( File storeFile, String expectedVersion )
    {
        StoreChannel fileChannel = null;
        String storeFilename = storeFile.getName();
        byte[] expectedVersionBytes = UTF8.encode( expectedVersion );
        try
        {
            if ( !fs.fileExists( storeFile ) )
            {
                return new Result( Outcome.missingStoreFile, null, storeFilename );
            }

            fileChannel = fs.open( storeFile, "r" );
            if ( fileChannel.size() < expectedVersionBytes.length )
            {
                return new Result( Outcome.storeVersionNotFound, null, storeFilename );
            }

            String actualVersion = readVersion( fileChannel, expectedVersionBytes.length );
            if ( !actualVersion.startsWith( typeDescriptor( expectedVersion ) ) )
            {
                return new Result( Outcome.storeVersionNotFound, actualVersion, storeFilename );
            }

            if ( !expectedVersion.equals( actualVersion ) )
            {
                return new Result( Outcome.unexpectedUpgradingStoreVersion, actualVersion, storeFilename );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            if ( fileChannel != null )
            {
                try
                {
                    fileChannel.close();
                }
                catch ( IOException e )
                {
                    // Ignore exception on close
                }
            }
        }
        return new Result( Outcome.ok, null, storeFilename );
    }

    private String typeDescriptor( String expectedVersion )
    {
        int spaceIndex = expectedVersion.indexOf( ' ' );
        if ( spaceIndex == -1 )
        {
            throw new IllegalArgumentException( "Unexpected version " + expectedVersion );
        }
        return expectedVersion.substring( 0, spaceIndex );
    }

    private String readVersion( StoreChannel fileChannel, int bytesToRead ) throws IOException
    {
        fileChannel.position( fileChannel.size() - bytesToRead );
        byte[] foundVersionBytes = new byte[bytesToRead];
        fileChannel.read( ByteBuffer.wrap( foundVersionBytes ) );
        return UTF8.decode( foundVersionBytes );
    }

    public static class Result
    {
        public final Outcome outcome;
        public final String actualVersion;
        public final String storeFilename;

        public Result( Outcome outcome, String actualVersion, String storeFilename )
        {
            this.outcome = outcome;
            this.actualVersion = actualVersion;
            this.storeFilename = storeFilename;
        }

        public static enum Outcome
        {
            ok( true ),
            missingStoreFile( false ),
            storeVersionNotFound( false ),
            unexpectedUpgradingStoreVersion( false );

            private final boolean success;

            private Outcome( boolean success )
            {
                this.success = success;
            }

            public boolean isSuccessful()
            {
                return this.success;
            }
        }
    }
}
