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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.util.FileUtils;

public class PropertyMigrationTest
{
    @SuppressWarnings({"unchecked"})
    @Test
    public void shouldRewrite() throws IOException
    {
        URL legacyStoreResource = getClass().getResource( "oldformatstore/neostore" );

        LegacyStore legacyStore = new LegacyStore( legacyStoreResource.getFile() );
        File legacyDir = new File( legacyStoreResource.getFile() ).getParentFile();

        LegacyPropertyStoreReader propertyStoreReader = legacyStore.getPropertyStoreReader();
        LegacyDynamicRecordFetcher legacyDynamicRecordFetcher = legacyStore.getDynamicRecordFetcher();
        LegacyNodeStoreReader legacyNodeStoreReader = legacyStore.getNodeStoreReader();

        HashMap config = MigrationTestUtils.defaultConfig();
        File outputDir = new File( "target/outputDatabase" );
        FileUtils.deleteRecursively( outputDir );
        assertTrue( outputDir.mkdirs() );

        String storeFileName = "target/outputDatabase/neostore";
        config.put( "neo_store", storeFileName );
        NeoStore.createStore( storeFileName, config );
        NeoStore neoStore = new NeoStore( config );

        new PropertyMigration( legacyNodeStoreReader, propertyStoreReader, legacyDynamicRecordFetcher ).migrateNodeProperties( neoStore.getNodeStore(), new PropertyWriter( neoStore.getPropertyStore() ) );
        new PropertyIndexMigration( legacyStore ).migratePropertyIndexes( neoStore );

        neoStore.close();

        copyInFilesThatAreNotMigrated( legacyDir, outputDir );
        verifyThatAllNodesHaveTheCorrectProperties( outputDir );

    }

    private void copyInFilesThatAreNotMigrated( File legacyDir, File outputDir )
    {
        for ( File legacyFile : legacyDir.listFiles() )
        {
            if (legacyFile.isDirectory()) {
                copyInFilesThatAreNotMigrated( legacyFile, new File( outputDir, legacyFile.getName() ) );
            } else {
                File outputFile = new File( outputDir, legacyFile.getName() );
                if ( !outputFile.exists() )
                {
                    try
                    {
                        FileUtils.copyFile( legacyFile, outputFile );
                    } catch ( IOException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
            }
        }
    }

    private void verifyThatAllNodesHaveTheCorrectProperties( File directory )
    {
        EmbeddedGraphDatabase database = new EmbeddedGraphDatabase( directory.getPath() );
        int nodeCount = 0;

        String longString = makeLongString();
        int[] longArray = makeLongArray();

        for ( Node node : database.getAllNodes() )
        {
            nodeCount++;
            if (node.getId() > 0) {
                assertEquals( Integer.MAX_VALUE, node.getProperty( PropertyType.INT.name() ) );
                assertEquals( longString, node.getProperty( PropertyType.STRING.name() ) );
                assertEquals( true, node.getProperty( PropertyType.BOOL.name() ) );
                assertEquals( Double.MAX_VALUE, node.getProperty( PropertyType.DOUBLE.name() ) );
                assertEquals( Float.MAX_VALUE, node.getProperty( PropertyType.FLOAT.name() ) );
                assertEquals( Long.MAX_VALUE, node.getProperty( PropertyType.LONG.name() ) );
                assertEquals( Byte.MAX_VALUE, node.getProperty( PropertyType.BYTE.name() ) );
                assertEquals( Character.MAX_VALUE, node.getProperty( PropertyType.CHAR.name() ) );
                assertArrayEquals( longArray, (int[]) node.getProperty( PropertyType.ARRAY.name() ) );
                assertEquals( Short.MAX_VALUE, node.getProperty( PropertyType.SHORT.name() ) );
                assertEquals( "short", node.getProperty( PropertyType.SHORT_STRING.name() ) );
            }
        }
        assertEquals( 1000, nodeCount );
        database.shutdown();
    }

    public static int[] makeLongArray()
    {
        int[] longArray = new int[100];
        for (int i = 0; i < 100; i++) {
            longArray[i] = i;
        }
        return longArray;
    }

    private static String makeLongString()
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            builder.append("characters");
        }
        return builder.toString();
    }

}
