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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.OpenOption;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStoreOrConfig;

@EphemeralPageCacheExtension
class StoreFactoryTest
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private PageCache pageCache;

    private NeoStores neoStores;
    private IdGeneratorFactory idGeneratorFactory;

    @BeforeEach
    void setUp()
    {
        idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem );
    }

    private StoreFactory storeFactory( Config config, OpenOption... openOptions )
    {
        LogProvider logProvider = NullLogProvider.getInstance();
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        RecordFormats recordFormats = selectForStoreOrConfig( config, databaseLayout, fileSystem, pageCache, logProvider );
        return new StoreFactory( databaseLayout, config, idGeneratorFactory, pageCache, fileSystem,
                recordFormats, logProvider, openOptions );
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
        neoStores = storeFactory( Config.defaults() ).openAllNeoStores( true );
        MetaDataStore metaDataStore = neoStores.getMetaDataStore();

        // Then
        assertThat( metaDataStore.getUpgradeTime(), equalTo( metaDataStore.getCreationTime() ) );
    }

    @Test
    void shouldHaveSameCommittedTransactionAndUpgradeTransactionOnStartup()
    {
        // When
        neoStores = storeFactory( Config.defaults() ).openAllNeoStores( true );
        MetaDataStore metaDataStore = neoStores.getMetaDataStore();

        // Then
        assertEquals( metaDataStore.getUpgradeTransaction(), metaDataStore.getLastCommittedTransaction() );
    }

    @Test
    void shouldThrowWhenOpeningNonExistingNeoStores()
    {
        assertThrows( StoreNotFoundException.class, () ->
        {
            try ( NeoStores neoStores = storeFactory( Config.defaults() ).openAllNeoStores() )
            {
                neoStores.getMetaDataStore();
            }
        } );
    }

    @Test
    void shouldDelegateDeletionOptionToStores()
    {
        // GIVEN
        StoreFactory storeFactory = storeFactory( Config.defaults(), DELETE_ON_CLOSE );

        // WHEN
        neoStores = storeFactory.openAllNeoStores( true );
        assertTrue( fileSystem.listFiles( testDirectory.databaseDir() ).length >= StoreType.values().length );

        // THEN
        neoStores.close();
        assertEquals( 0, fileSystem.listFiles( testDirectory.databaseDir() ).length );
    }

    @Test
    void shouldHandleStoreConsistingOfOneEmptyFile() throws Exception
    {
        StoreFactory storeFactory = storeFactory( Config.defaults() );
        fileSystem.write( testDirectory.databaseLayout().file( "neostore.nodestore.db.labels" ) );
        storeFactory.openAllNeoStores( true ).close();
    }

    @Test
    void shouldCompleteInitializationOfStoresWithIncompleteHeaders() throws Exception
    {
        StoreFactory storeFactory = storeFactory( Config.defaults() );
        storeFactory.openAllNeoStores( true ).close();
        for ( File f : fileSystem.listFiles( testDirectory.databaseDir() ) )
        {
            fileSystem.truncate( f, 0 );
        }
        storeFactory.openAllNeoStores( true ).close();
    }
}
