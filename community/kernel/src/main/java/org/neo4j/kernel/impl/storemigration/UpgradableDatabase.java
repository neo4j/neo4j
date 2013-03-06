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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyDynamicStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyNodeStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyPropertyIndexStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyPropertyStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipTypeStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;

/**
 * Logic to check whether a database version is upgradable to the current version. It looks at the
 * version information found in the store files themselves.
 */
public class UpgradableDatabase
{
    /*
     * Initialized by the static block below.
     */
    public static final Map<String, String> fileNamesToExpectedVersions;
    private final FileSystemAbstraction fs;

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
    
    public UpgradableDatabase( FileSystemAbstraction fs )
    {
        this.fs = fs;
    }

    public boolean storeFilesUpgradeable( File neoStoreFile )
    {
        try
		{
            checkUpgradeable( neoStoreFile );
            return true;
        }
		catch ( StoreUpgrader.UnableToUpgradeException e )
        {
            return false;
        }
    }

    public void checkUpgradeable( File neoStoreFile )
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
                if ( !fs.fileExists( storeFile ) )
                {
                    throw new StoreUpgrader.UnableToUpgradeException( String.format( "Missing required store file " +
                            "'%s'.", storeFile.getName() ) );
                }
                fileChannel = fs.open( storeFile, "r" );
                if ( fileChannel.size() < expectedVersionBytes.length )
                {
                    throw new StoreUpgrader.UnableToUpgradeException( String.format( "'%s' does not contain a store " +
                            "version, please ensure that the original database was shut down in a clean state.",
                            storeFile.getName() ) );
                }
                fileChannel.position( fileChannel.size() - expectedVersionBytes.length );
                byte[] foundVersionBytes = new byte[expectedVersionBytes.length];
                fileChannel.read( ByteBuffer.wrap( foundVersionBytes ) );
                if ( !expectedVersion.equals( UTF8.decode( foundVersionBytes ) ) )
                {
                    throw new StoreUpgrader.UnableToUpgradeException( String.format(
                            "'%s' has a store version number that we cannot upgrade from. " +
                            "Expected '%s' but file is version '%s'.",
                            storeFile.getName(), expectedVersion, UTF8.decode( foundVersionBytes ) ) );
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
        }
    }
}
