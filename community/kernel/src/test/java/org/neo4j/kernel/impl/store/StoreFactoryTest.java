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
package org.neo4j.kernel.impl.store;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StoreFactoryTest
{
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );
    private StoreFactory storeFactory;
    private NeoStores neoStores;
    private File storeDir;

    @Before
    public void setUp() throws IOException
    {
        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs );

        storeDir = testDirectory.graphDbDir();
        fs.mkdirs( storeDir );
        storeFactory = new StoreFactory( storeDir, new Config(), idGeneratorFactory, pageCache,
                fs, NullLogProvider.getInstance() );
    }

    @After
    public void tearDown()
    {
        if ( neoStores != null )
        {
            neoStores.close();
        }
    }

    @Test
    public void shouldHaveSameCreationTimeAndUpgradeTimeOnStartup() throws Exception
    {
        // When
        neoStores = storeFactory.openAllNeoStores( true );
        MetaDataStore metaDataStore = neoStores.getMetaDataStore();

        // Then
        assertThat( metaDataStore.getUpgradeTime(), equalTo( metaDataStore.getCreationTime() ) );
    }

    @Test
    public void shouldHaveSameCommittedTransactionAndUpgradeTransactionOnStartup() throws Exception
    {
        // When
        neoStores = storeFactory.openAllNeoStores( true );
        MetaDataStore metaDataStore = neoStores.getMetaDataStore();

        // Then
        assertEquals( metaDataStore.getUpgradeTransaction(), metaDataStore.getLastCommittedTransaction() );
    }

    @Test
    public void shouldHaveSpecificCountsTrackerForReadOnlyDatabase() throws IOException
    {
        // when
        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        StoreFactory readOnlyStoreFactory = new StoreFactory( testDirectory.directory( "readOnlyStore" ),
                new Config( MapUtil.stringMap( GraphDatabaseSettings.read_only.name(), Settings.TRUE ) ),
                new DefaultIdGeneratorFactory( fs ), pageCache, fs, NullLogProvider.getInstance() );
        neoStores = readOnlyStoreFactory.openAllNeoStores( true );
        long lastClosedTransactionId = neoStores.getMetaDataStore().getLastClosedTransactionId();

        // then
        assertEquals( -1, neoStores.getCounts().rotate( lastClosedTransactionId ) );
    }

    @Test( expected = StoreNotFoundException.class )
    public void shouldThrowWhenOpeningNonExistingNeoStores()
    {
        try ( NeoStores neoStores = storeFactory.openAllNeoStores() )
        {
            neoStores.getMetaDataStore();
        }
    }
}
