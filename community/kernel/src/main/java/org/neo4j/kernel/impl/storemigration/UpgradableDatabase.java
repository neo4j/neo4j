/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyDynamicStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyNodeStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyPropertyIndexStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyPropertyStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipTypeStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;

public class UpgradableDatabase
{
    /*
     * Initialized by the static block below.
     */
    public static final Map<String, String> fileNamesToExpectedVersions;

    static
    {
        Map<String, String> before = new HashMap<String, String>();
        before.put( NeoStore.DEFAULT_NAME, LegacyStore.FROM_VERSION );
        before.put( "neostore.nodestore.db", LegacyNodeStoreReader.FROM_VERSION );
        before.put( "neostore.propertystore.db",
                LegacyPropertyStoreReader.FROM_VERSION );
        before.put( "neostore.propertystore.db.arrays",
                LegacyDynamicStoreReader.FROM_VERSION_ARRAY );
        before.put( "neostore.propertystore.db.index",
                LegacyPropertyIndexStoreReader.FROM_VERSION );
        before.put( "neostore.propertystore.db.index.keys",
                LegacyDynamicStoreReader.FROM_VERSION_STRING );
        before.put( "neostore.propertystore.db.strings",
                LegacyDynamicStoreReader.FROM_VERSION_STRING );
        before.put( "neostore.relationshipstore.db",
                LegacyRelationshipStoreReader.FROM_VERSION );
        before.put( "neostore.relationshiptypestore.db",
                LegacyRelationshipTypeStoreReader.FROM_VERSION );
        before.put( "neostore.relationshiptypestore.db.names",
                LegacyDynamicStoreReader.FROM_VERSION_STRING );
        fileNamesToExpectedVersions = Collections.unmodifiableMap( before );
    }

    public void checkUpgradeable( File neoStoreFile )
    {
        if (!storeFilesUpgradeable( neoStoreFile ))
        {
            throw new StoreUpgrader.UnableToUpgradeException( "Not all store files match the version required for successful upgrade" );
        }
    }

    public boolean storeFilesUpgradeable( File neoStoreFile )
    {
        File storeDirectory = neoStoreFile.getParentFile();
        for ( String fileName : fileNamesToExpectedVersions.keySet() )
        {
            String expectedVersion = fileNamesToExpectedVersions.get( fileName );
            FileChannel fileChannel = null;
            byte[] expectedVersionBytes = UTF8.encode( expectedVersion );
            try
            {
                File storeFile = new File( storeDirectory, fileName );
                if (!storeFile.exists()) {
                    return false;
                }
                fileChannel = new RandomAccessFile( storeFile, "r" ).getChannel();
                if ( fileChannel.size() < expectedVersionBytes.length ) {
                    return false;
                }
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
