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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StoreFactoryTest
{
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );
    private StoreFactory storeFactory;
    private NeoStore neoStore;

    @Before
    public void setUp()
    {
        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        PageCache pageCache = pageCacheRule.getPageCache( fs );

        storeFactory = new StoreFactory( testDirectory.graphDbDir(), new Config(),
                new DefaultIdGeneratorFactory(), pageCache, fs, NullLogProvider.getInstance(), new Monitors() );
    }

    @After
    public void tearDown()
    {
        if ( neoStore != null )
        {
            neoStore.close();
        }
    }

    @Test
    public void shouldHaveSameCreationTimeAndUpgradeTimeOnStartup() throws Exception
    {
        // When
        neoStore = storeFactory.createNeoStore();

        // Then
        assertThat( neoStore.getUpgradeTime(), equalTo( neoStore.getCreationTime() ) );
    }

    @Test
    public void shouldHaveSameCommittedTransactionAndUpgradeTransactionOnStartup() throws Exception
    {
        // When
        neoStore = storeFactory.createNeoStore();

        // Then
        long[] lastCommittedTransaction = neoStore.getLastCommittedTransaction();
        long[] txIdChecksum = new long[2];
        System.arraycopy( lastCommittedTransaction, 0, txIdChecksum, 0, 2 );
        assertArrayEquals( neoStore.getUpgradeTransaction(), txIdChecksum );
    }

    @Test
    public void shouldHaveSpecificCountsTrackerForReadOnlyDatabase() throws IOException
    {
        // when
        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        StoreFactory readOnlyStoreFactory = new StoreFactory( testDirectory.directory( "readOnlyStore" ),
                new Config( MapUtil.stringMap( GraphDatabaseSettings.read_only.name(), Settings.TRUE ) ),
                new DefaultIdGeneratorFactory(), pageCache, fs, NullLogProvider.getInstance(), new Monitors() );
        neoStore = readOnlyStoreFactory.createNeoStore();
        long lastClosedTransactionId = neoStore.getLastClosedTransactionId();

        // then
        assertEquals( -1, neoStore.getCounts().rotate( lastClosedTransactionId ));
    }
}
