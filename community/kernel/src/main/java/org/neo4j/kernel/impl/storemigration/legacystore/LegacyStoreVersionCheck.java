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
package org.neo4j.kernel.impl.storemigration.legacystore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.helpers.UTF8;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck.Result;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck.Result.Outcome;

public class LegacyStoreVersionCheck
{
    private final FileSystemAbstraction fs;

    public LegacyStoreVersionCheck( FileSystemAbstraction fs )
    {
        this.fs = fs;
    }

    public Result hasVersion( File storeFile, String expectedVersion, boolean optional )
    {
        StoreChannel fileChannel = null;
        String storeFilename = storeFile.getName();
        byte[] expectedVersionBytes = UTF8.encode( expectedVersion );
        try
        {
            if ( !fs.fileExists( storeFile ) )
            {
                if ( optional )
                {
                    return new Result( Outcome.ok, null, storeFilename );
                }

                return new Result( Outcome.missingStoreFile, null, storeFilename );
            }

            fileChannel = fs.open( storeFile, "r" );
            if ( fileChannel.size() < expectedVersionBytes.length )
            {
                return new Result( Outcome.storeVersionNotFound, null, storeFilename );
            }

            String actualVersion = readVersion( fileChannel, expectedVersionBytes.length );
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

    private String readVersion( StoreChannel fileChannel, int bytesToRead ) throws IOException
    {
        fileChannel.position( fileChannel.size() - bytesToRead );
        byte[] foundVersionBytes = new byte[bytesToRead];
        fileChannel.read( ByteBuffer.wrap( foundVersionBytes ) );
        return UTF8.decode( foundVersionBytes );
    }
}
