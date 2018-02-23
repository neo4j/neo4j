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

import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.read_only;
import static org.neo4j.kernel.configuration.Config.defaults;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.impl.store.MetaDataStore.DEFAULT_NAME;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStoreOrConfig;
import static org.neo4j.logging.NullLogProvider.getInstance;

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

    @BeforeEach
    void setUp()
    {
        FileSystemAbstraction fs = fsRule.get();
        pageCache = pageCacheRule.getPageCache( fs );
        idGeneratorFactory = new DefaultIdGeneratorFactory( fs );
        storeDir = directory( "dir" );
    }

    private StoreFactory storeFactory( Config config, OpenOption... openOptions )
    {
        LogProvider logProvider = getInstance();
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

    @AfterEach
    void tearDown()
    {
        if ( neoStores != null )
        {
            neoStores.close();
        }
    }

    @Test
    void shouldHaveSameCreationTimeAndUpgradeTimeOnStartup()
    {
        // When
        neoStores = storeFactory( defaults() ).openAllNeoStores( true );
        MetaDataStore metaDataStore = neoStores.getMetaDataStore();

        // Then
        assertThat( metaDataStore.getUpgradeTime(), equalTo( metaDataStore.getCreationTime() ) );
    }

    @Test
    void shouldHaveSameCommittedTransactionAndUpgradeTransactionOnStartup()
    {
        // When
        neoStores = storeFactory( defaults() ).openAllNeoStores( true );
        MetaDataStore metaDataStore = neoStores.getMetaDataStore();

        // Then
        assertEquals( metaDataStore.getUpgradeTransaction(), metaDataStore.getLastCommittedTransaction() );
    }

    @Test
    void shouldHaveSpecificCountsTrackerForReadOnlyDatabase() throws IOException
    {
        // when
        StoreFactory readOnlyStoreFactory = storeFactory( defaults( read_only, TRUE ) );
        neoStores = readOnlyStoreFactory.openAllNeoStores( true );
        long lastClosedTransactionId = neoStores.getMetaDataStore().getLastClosedTransactionId();

        // then
        assertEquals( -1, neoStores.getCounts().rotate( lastClosedTransactionId ) );
    }

    @Test
    void shouldThrowWhenOpeningNonExistingNeoStores()
    {
        assertThrows( StoreNotFoundException.class, () -> {
            try ( NeoStores neoStores = storeFactory( defaults() ).openAllNeoStores() )
            {
                neoStores.getMetaDataStore();
            }
        } );
    }

    @Test
    void shouldDelegateDeletionOptionToStores()
    {
        // GIVEN
        StoreFactory storeFactory = storeFactory( defaults(), DELETE_ON_CLOSE );

        // WHEN
        neoStores = storeFactory.openAllNeoStores( true );
        assertTrue( fsRule.get().listFiles( storeDir ).length >= StoreType.values().length );

        // THEN
        neoStores.close();
        assertEquals( 0, fsRule.get().listFiles( storeDir ).length );
    }

    @Test
    void shouldHandleStoreConsistingOfOneEmptyFile() throws Exception
    {
        StoreFactory storeFactory = storeFactory( defaults() );
        FileSystemAbstraction fs = fsRule.get();
        fs.create( new File( storeDir, "neostore.nodestore.db.labels" ) );
        storeFactory.openAllNeoStores( true ).close();
    }

    @Test
    void shouldCompleteInitializationOfStoresWithIncompleteHeaders() throws Exception
    {
        StoreFactory storeFactory = storeFactory( defaults() );
        storeFactory.openAllNeoStores( true ).close();
        FileSystemAbstraction fs = fsRule.get();
        for ( File f : fs.listFiles( storeDir ) )
        {
            fs.truncate( f, 0 );
        }
        storeFactory.openAllNeoStores( true ).close();
    }
}
