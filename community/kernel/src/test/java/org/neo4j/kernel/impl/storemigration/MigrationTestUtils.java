/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.util.FileUtils;

public class MigrationTestUtils
{
    public static Config defaultConfig()
    {
        return defaultConfig( MapUtil.stringMap() );
    }

    public static Config defaultConfig( Map<String, String> inputParams )
    {
        return new Config( inputParams, GraphDatabaseSettings.class );
    }

    public static int[] makeLongArray()
    {
        int[] longArray = new int[100];
        for ( int i = 0; i < 100; i++ )
        {
            longArray[i] = i;
        }
        return longArray;
    }

    static String makeLongString()
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < 100; i++ )
        {
            builder.append( "characters" );
        }
        return builder.toString();
    }

    static void changeVersionNumber( File storeFile, String versionString ) throws IOException
    {
        byte[] versionBytes = UTF8.encode( versionString );
        FileChannel fileChannel = new RandomAccessFile( storeFile, "rw" ).getChannel();
        fileChannel.position( storeFile.length() - versionBytes.length );
        fileChannel.write( ByteBuffer.wrap( versionBytes ) );
        fileChannel.close();
    }

    static void truncateFile( File storeFile, String suffixToDetermineTruncationLength ) throws IOException
    {
        byte[] versionBytes = UTF8.encode( suffixToDetermineTruncationLength );
        FileChannel fileChannel = new RandomAccessFile( storeFile, "rw" ).getChannel();
        fileChannel.truncate( storeFile.length() - versionBytes.length );
        fileChannel.close();
    }

    static void truncateToFixedLength( File storeFile, int newLength ) throws IOException
    {
        FileChannel fileChannel = new RandomAccessFile( storeFile, "rw" ).getChannel();
        fileChannel.truncate( newLength );
        fileChannel.close();
    }

    public static void prepareSampleLegacyDatabase( File workingDirectory ) throws IOException
    {
        File resourceDirectory = findOldFormatStoreDirectory();

        deleteRecursively( workingDirectory );
        assertTrue( workingDirectory.mkdirs() );

        FileUtils.copyRecursively( resourceDirectory, workingDirectory );
    }

    public static File findOldFormatStoreDirectory()
    {
        URL legacyStoreResource = LegacyStore.class.getResource( "exampledb/neostore" );
        return new File( legacyStoreResource.getFile() ).getParentFile();
    }

    public static boolean allStoreFilesHaveVersion( File workingDirectory, String version ) throws IOException
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

    public static void verifyFilesHaveSameContent( File original, File other ) throws IOException
    {
        for ( File originalFile : original.listFiles() )
        {
            File otherFile = new File( other, originalFile.getName() );
            if ( !originalFile.isDirectory() )
            {
                BufferedInputStream originalStream = new BufferedInputStream( new FileInputStream( originalFile ) );
                BufferedInputStream otherStream = new BufferedInputStream( new FileInputStream( otherFile ) );

                int aByte;
                while ( (aByte = originalStream.read()) != -1 )
                {
                    assertEquals( "Different content in " + originalFile.getName(), aByte, otherStream.read() );
                }

                originalStream.close();
                otherStream.close();
            }
        }
    }

    static class AlwaysAllowedUpgradeConfiguration implements UpgradeConfiguration
    {
        public void checkConfigurationAllowsAutomaticUpgrade()
        {
        }
    }

    public static UpgradeConfiguration alwaysAllowed()
    {
        return new AlwaysAllowedUpgradeConfiguration();
    }
}
