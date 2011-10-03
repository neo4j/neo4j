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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.defaultConfig;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.FileUtils;

public class StoreUpgraderTest
{
    @Test
    public void shouldUpgradeAnOldFormatStore() throws IOException
    {
        File workingDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName() );
        prepareSampleLegacyDatabase( workingDirectory );

        assertFalse( allStoreFilesHaveVersion( workingDirectory, ALL_STORES_VERSION ) );

        HashMap config = defaultConfig();
        config.put( Config.ALLOW_STORE_UPGRADE, "true" );

        new StoreUpgrader( new File( workingDirectory, "neostore" ).getPath(), config ).attemptUpgrade();

        assertTrue( allStoreFilesHaveVersion( workingDirectory, ALL_STORES_VERSION ) );
    }

    @Test
    @Ignore("In progress")
    public void shouldLeaveACopyOfOriginalStoreFilesInBackupDirectory() throws IOException
    {
        File workingDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName() );
        prepareSampleLegacyDatabase( workingDirectory );

        HashMap config = defaultConfig();
        config.put( Config.ALLOW_STORE_UPGRADE, "true" );

        new StoreUpgrader( new File( workingDirectory, "neostore" ).getPath(), config ).attemptUpgrade();

        verifyFilesHaveSameContent( findOldFormatStoreDirectory(), new File( workingDirectory, "upgrade_backup" ) );
    }

    private void verifyFilesHaveSameContent( File original, File other ) throws IOException
    {
        for ( File originalFile : original.listFiles() )
        {
            File otherFile = new File( other, originalFile.getName() );
            if (originalFile.isDirectory())
            {
                verifyFilesHaveSameContent( originalFile, otherFile );
            }
            else
            {
                BufferedInputStream originalStream = new BufferedInputStream( new FileInputStream( originalFile ) );
                BufferedInputStream otherStream = new BufferedInputStream( new FileInputStream( otherFile ) );

                int aByte;
                while( (aByte = originalStream.read()) != -1)
                {
                    assertEquals( aByte, otherStream.read() );
                }

                originalStream.close();
                otherStream.close();
            }
        }
    }

    @Test
    public void shouldUpgradeAutomaticallyOnDatabaseStartup() throws IOException
    {
        File workingDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName() );
        prepareSampleLegacyDatabase( workingDirectory );

        assertFalse( allStoreFilesHaveVersion( workingDirectory, ALL_STORES_VERSION ) );

        HashMap params = new HashMap();
        params.put( Config.ALLOW_STORE_UPGRADE, "true" );

        GraphDatabaseService database = new EmbeddedGraphDatabase( workingDirectory.getPath(), params );
        database.shutdown();

        assertTrue( allStoreFilesHaveVersion( workingDirectory, ALL_STORES_VERSION ) );
    }

    @Test
    public void shouldFailToUpgradeIfConfigParameterIsMissing() throws IOException
    {
        File workingDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName() );
        prepareSampleLegacyDatabase( workingDirectory );

        HashMap config = defaultConfig();
        assertFalse( config.containsKey( Config.ALLOW_STORE_UPGRADE ) );

        try {
            new StoreUpgrader( new File( workingDirectory, "neostore" ).getPath(), config ).attemptUpgrade();
            fail( "Should throw exception" );
        }
        catch ( UpgradeNotAllowedByConfigurationException e )
        {
            //expected
        }
    }

    private void prepareSampleLegacyDatabase( File workingDirectory ) throws IOException
    {
        File resourceDirectory = findOldFormatStoreDirectory();

        FileUtils.deleteRecursively( workingDirectory );
        assertTrue( workingDirectory.mkdirs() );

        MigrationTestUtils.copyRecursively( resourceDirectory, workingDirectory );
    }

    private File findOldFormatStoreDirectory()
    {
        URL legacyStoreResource = getClass().getResource( "oldformatstore/neostore" );
        return new File( legacyStoreResource.getFile() ).getParentFile();
    }

    private boolean allStoreFilesHaveVersion( File workingDirectory, String version ) throws IOException
    {

        for ( String fileName : StoreFiles.fileNames )
        {
            FileChannel channel = new RandomAccessFile( new File( workingDirectory, fileName ), "r" ).getChannel();
            int length = UTF8.encode( version ).length;
            byte[] bytes = new byte[length];
            ByteBuffer buffer = ByteBuffer.wrap( bytes );
            channel.position( channel.size() - length );
            channel.read( buffer );
            channel.close();

            String foundVersion = UTF8.decode( bytes );
            if ( !version.equals( foundVersion ) )
            {
                return false;
            }
        }
        return true;
    }

}
