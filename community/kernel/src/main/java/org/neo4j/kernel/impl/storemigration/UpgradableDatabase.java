/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;

public class UpgradableDatabase
{
    private Map<String, String> fileNamesToExpectedVersions = new HashMap<String, String>();

    public UpgradableDatabase()
    {
        fileNamesToExpectedVersions.put( NeoStore.DEFAULT_NAME, "NeoStore v0.9.9" );
        fileNamesToExpectedVersions.put( "neostore.nodestore.db", "NodeStore v0.9.9" );
        fileNamesToExpectedVersions.put( "neostore.propertystore.db", "PropertyStore v0.9.9" );
        fileNamesToExpectedVersions.put( "neostore.propertystore.db.arrays", "ArrayPropertyStore v0.9.9" );
        fileNamesToExpectedVersions.put( "neostore.propertystore.db.index", "PropertyIndex v0.9.9" );
        fileNamesToExpectedVersions.put( "neostore.propertystore.db.index.keys", "StringPropertyStore v0.9.9" );
        fileNamesToExpectedVersions.put( "neostore.propertystore.db.strings", "StringPropertyStore v0.9.9" );
        fileNamesToExpectedVersions.put( "neostore.relationshipstore.db", "RelationshipStore v0.9.9" );
        fileNamesToExpectedVersions.put( "neostore.relationshiptypestore.db", "RelationshipTypeStore v0.9.9" );
        fileNamesToExpectedVersions.put( "neostore.relationshiptypestore.db.names", "StringPropertyStore v0.9.9" );
    }

    public void checkUpgradeable( File storeFile )
    {
        if (!storeFilesUpgradeable( storeFile ))
        {
            throw new StoreUpgrader.UnableToUpgradeException();
        }
    }

    public boolean storeFilesUpgradeable( File storeFile )
    {
        File storeDirectory = storeFile.getParentFile();
        for ( String fileName : fileNamesToExpectedVersions.keySet() )
        {
            String expectedVersion = fileNamesToExpectedVersions.get( fileName );
            FileChannel fileChannel = null;
            byte[] expectedVersionBytes = UTF8.encode( expectedVersion );
            try
            {
                fileChannel = new RandomAccessFile( new File( storeDirectory, fileName ), "r" ).getChannel();
                fileChannel.position( fileChannel.size() - expectedVersionBytes.length );
                byte[] foundVersionBytes = new byte[expectedVersionBytes.length];
                fileChannel.read( ByteBuffer.wrap( foundVersionBytes ) );
                if ( !expectedVersion.equals( UTF8.decode( foundVersionBytes ) ) )
                {
                    return false;
                }
            } catch ( IOException e )
            {
                throw new RuntimeException( e );
            } finally
            {
                if ( fileChannel != null )
                {
                    try
                    {
                        fileChannel.close();
                    } catch ( IOException e )
                    {
                        // Ignore exception on close
                    }
                }
            }
        }
        return true;
    }
}
