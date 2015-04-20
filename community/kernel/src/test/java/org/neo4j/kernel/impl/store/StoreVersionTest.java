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
package org.neo4j.kernel.impl.store;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStore.Position;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.store.StoreFactory.configForStoreDir;

public class StoreVersionTest
{
    @Test
    public void allStoresShouldHaveTheCurrentVersionIdentifier()
    {
        StoreFactory sf = new StoreFactory(
                config,
                new DefaultIdGeneratorFactory(),
                pageCacheRule.getPageCache( fs.get() ),
                fs.get(),
                StringLogger.DEV_NULL,
                monitors );
        NeoStore neoStore = sf.newNeoStore( true );

        CommonAbstractStore[] stores = {
                neoStore.getNodeStore(),
                neoStore.getRelationshipStore(),
                neoStore.getRelationshipTypeTokenStore(),
                neoStore.getPropertyStore(),
                neoStore.getPropertyKeyTokenStore()
        };

        for ( CommonAbstractStore store : stores )
        {
            assertThat( store.getTypeAndVersionDescriptor(), containsString( CommonAbstractStore.ALL_STORES_VERSION ) );
        }
        neoStore.close();
    }

    @Test
    @Ignore
    public void shouldFailToCreateAStoreContainingOldVersionNumber() throws IOException
    {
        URL legacyStoreResource = StoreMigrator.class.getResource( "legacystore/exampledb/neostore.nodestore.db" );
        File workingFile = new File( outputDir, "neostore.nodestore.db" );
        FileUtils.copyFile( new File( legacyStoreResource.getFile() ), workingFile );

        Config config = new Config( new HashMap<String, String>(), GraphDatabaseSettings.class );

        try
        {
            Monitors monitors = new Monitors();
            new NodeStore(
                    workingFile,
                    config,
                    new DefaultIdGeneratorFactory(),
                    pageCacheRule.getPageCache( fs.get() ),
                    fs.get(),
                    StringLogger.DEV_NULL,
                    null,
                    StoreVersionMismatchHandler.FORCE_CURRENT_VERSION,
                    monitors );
            fail( "Should have thrown exception" );
        }
        catch ( NotCurrentStoreVersionException e )
        {
            //expected
        }
    }

    @Test
    public void neoStoreHasCorrectStoreVersionField()
    {
        StoreFactory sf = new StoreFactory(
                config,
                new DefaultIdGeneratorFactory(),
                pageCacheRule.getPageCache( fs.get() ),
                fs.get(),
                StringLogger.DEV_NULL,
                monitors );
        NeoStore neoStore = sf.newNeoStore( true );

        // The first checks the instance method, the other the public one
        assertEquals( CommonAbstractStore.ALL_STORES_VERSION,
                NeoStore.versionLongToString( neoStore.getStoreVersion() ) );
        assertEquals( CommonAbstractStore.ALL_STORES_VERSION,
                NeoStore.versionLongToString( NeoStore.getRecord( fs.get(),
                        new File( outputDir, NeoStore.DEFAULT_NAME ), Position.STORE_VERSION ) ) );
        neoStore.close();
    }

    @Test
    public void testProperEncodingDecodingOfVersionString()
    {
        String[] toTest = new String[]{"123", "foo", "0.9.9", "v0.A.0",
                "bar", "chris", "1234567"};
        for ( String string : toTest )
        {
            assertEquals(
                    string,
                    NeoStore.versionLongToString( NeoStore.versionStringToLong( string ) ) );
        }
    }

    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final File outputDir = new File( "target/var/" + StoreVersionTest.class.getSimpleName() ).getAbsoluteFile();
    private final Config config = configForStoreDir(
            new Config( stringMap(), GraphDatabaseSettings.class ), outputDir );
    private final Monitors monitors = new Monitors();
    @ClassRule
    public static PageCacheRule pageCacheRule = new PageCacheRule();
}
