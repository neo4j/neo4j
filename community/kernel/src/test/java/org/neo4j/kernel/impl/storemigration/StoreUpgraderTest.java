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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.defaultConfig;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.Test;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.util.FileUtils;

public class StoreUpgraderTest
{
    @Test
    public void shouldUpgradeAnOldFormatStore() throws IOException
    {
        URL legacyStoreResource = getClass().getResource( "oldformatstore/neostore" );
        File resourceDirectory = new File( legacyStoreResource.getFile() ).getParentFile();
        File workingDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName() );

        FileUtils.deleteRecursively( workingDirectory );
        assertTrue( workingDirectory.mkdirs() );

        copyRecursively( resourceDirectory, workingDirectory );

        assertFalse( allStoreFilesHaveVersion( workingDirectory, ALL_STORES_VERSION ) );

        new StoreUpgrader( new File( workingDirectory, "neostore" ).getPath(), defaultConfig() ).attemptUpgrade();

        assertTrue( allStoreFilesHaveVersion( workingDirectory, ALL_STORES_VERSION ) );
    }

    private boolean allStoreFilesHaveVersion( File workingDirectory, String version ) throws IOException
    {
        String[] fileNames = {
                "neostore",
                "neostore.nodestore.db",
                "neostore.propertystore.db",
                "neostore.propertystore.db.arrays",
                "neostore.propertystore.db.index",
                "neostore.propertystore.db.index.keys",
                "neostore.propertystore.db.strings",
                "neostore.relationshipstore.db",
                "neostore.relationshiptypestore.db",
                "neostore.relationshiptypestore.db.names",
        };

        for ( String fileName : fileNames )
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

    private void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
    {
        for ( File fromFile : fromDirectory.listFiles() )
        {
            File toFile = new File( toDirectory, fromFile.getName() );
            if ( fromFile.isDirectory() )
            {
                assertTrue( toFile.mkdir() );
                copyRecursively( fromFile, toFile );
            } else
            {
                FileUtils.copyFile( fromFile, toFile );
            }
        }
    }
}
