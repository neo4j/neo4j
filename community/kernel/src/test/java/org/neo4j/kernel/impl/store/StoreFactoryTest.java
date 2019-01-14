/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.nio.file.OpenOption;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.store.MetaDataStore.DEFAULT_NAME;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStoreOrConfig;

public class StoreFactoryTest
{
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private NeoStores neoStores;
    private File storeDir;
    private IdGeneratorFactory idGeneratorFactory;
    private PageCache pageCache;

    @Before
    public void setUp()
    {
        FileSystemAbstraction fs = fsRule.get();
        pageCache = pageCacheRule.getPageCache( fs );
        idGeneratorFactory = new DefaultIdGeneratorFactory( fs );
        storeDir = directory( "dir" );
    }

    private StoreFactory storeFactory( Config config, OpenOption... openOptions )
    {
        LogProvider logProvider = NullLogProvider.getInstance();
        RecordFormats recordFormats = selectForStoreOrConfig( config, storeDir, pageCache, logProvider );
        return new StoreFactory( storeDir, DEFAULT_NAME, config, idGeneratorFactory, pageCache, fsRule.get(),
                recordFormats, logProvider, EmptyVersionContextSupplier.EMPTY, openOptions );
    }

    private File directory( String name )
    {
        File dir = new File( name ).getAbsoluteFile();
        fsRule.get().mkdirs( dir );
        return dir;
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
    public void shouldHaveSameCreationTimeAndUpgradeTimeOnStartup()
    {
        // When
        neoStores = storeFactory( Config.defaults() ).openAllNeoStores( true );
        MetaDataStore metaDataStore = neoStores.getMetaDataStore();

        // Then
        assertThat( metaDataStore.getUpgradeTime(), equalTo( metaDataStore.getCreationTime() ) );
    }

    @Test
    public void shouldHaveSameCommittedTransactionAndUpgradeTransactionOnStartup()
    {
        // When
        neoStores = storeFactory( Config.defaults() ).openAllNeoStores( true );
        MetaDataStore metaDataStore = neoStores.getMetaDataStore();

        // Then
        assertEquals( metaDataStore.getUpgradeTransaction(), metaDataStore.getLastCommittedTransaction() );
    }

    @Test
    public void shouldHaveSpecificCountsTrackerForReadOnlyDatabase() throws IOException
    {
        // when
        StoreFactory readOnlyStoreFactory = storeFactory( Config.defaults( GraphDatabaseSettings.read_only, Settings.TRUE ) );
        neoStores = readOnlyStoreFactory.openAllNeoStores( true );
        long lastClosedTransactionId = neoStores.getMetaDataStore().getLastClosedTransactionId();

        // then
        assertEquals( -1, neoStores.getCounts().rotate( lastClosedTransactionId ) );
    }

    @Test( expected = StoreNotFoundException.class )
    public void shouldThrowWhenOpeningNonExistingNeoStores()
    {
        try ( NeoStores neoStores = storeFactory( Config.defaults() ).openAllNeoStores() )
        {
            neoStores.getMetaDataStore();
        }
    }

    @Test
    public void shouldDelegateDeletionOptionToStores()
    {
        // GIVEN
        StoreFactory storeFactory = storeFactory( Config.defaults(), DELETE_ON_CLOSE );

        // WHEN
        neoStores = storeFactory.openAllNeoStores( true );
        assertTrue( fsRule.get().listFiles( storeDir ).length >= StoreType.values().length );

        // THEN
        neoStores.close();
        assertEquals( 0, fsRule.get().listFiles( storeDir ).length );
    }

    @Test
    public void shouldHandleStoreConsistingOfOneEmptyFile() throws Exception
    {
        StoreFactory storeFactory = storeFactory( Config.defaults() );
        FileSystemAbstraction fs = fsRule.get();
        fs.create( new File( storeDir, "neostore.nodestore.db.labels" ) );
        storeFactory.openAllNeoStores( true ).close();
    }

    @Test
    public void shouldCompleteInitializationOfStoresWithIncompleteHeaders() throws Exception
    {
        StoreFactory storeFactory = storeFactory( Config.defaults() );
        storeFactory.openAllNeoStores( true ).close();
        FileSystemAbstraction fs = fsRule.get();
        for ( File f : fs.listFiles( storeDir ) )
        {
            fs.truncate( f, 0 );
        }
        storeFactory.openAllNeoStores( true ).close();
    }
}
