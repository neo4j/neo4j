/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.eclipse.collections.api.set.ImmutableSet;
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
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;

import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStoreOrConfig;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
class StoreFactoryTest
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;
    @Inject
    private DatabaseLayout databaseLayout;

    private NeoStores neoStores;
    private IdGeneratorFactory idGeneratorFactory;

    @BeforeEach
    void setUp()
    {
        idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, immediate() );
    }

    private StoreFactory storeFactory( Config config, PageCacheTracer pageCacheTracer )
    {
        return storeFactory( config, pageCacheTracer, immutable.empty() );
    }

    private StoreFactory storeFactory( Config config, PageCacheTracer pageCacheTracer, ImmutableSet<OpenOption> openOptions )
    {
        LogProvider logProvider = NullLogProvider.getInstance();
        RecordFormats recordFormats = selectForStoreOrConfig( config, databaseLayout, fileSystem, pageCache, logProvider, NULL );
        return new StoreFactory( databaseLayout, config, idGeneratorFactory, pageCache, fileSystem, recordFormats, logProvider, pageCacheTracer, openOptions );
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
    void tracePageCacheAccessOnOpenStores()
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        neoStores = storeFactory( defaults(), pageCacheTracer ).openAllNeoStores( true );

        assertThat( pageCacheTracer.pins() ).isNotZero();
        assertThat( pageCacheTracer.unpins() ).isNotZero();
    }

    @Test
    void shouldHaveSameCreationTimeAndUpgradeTimeOnStartup()
    {
        // When
        neoStores = storeFactory( defaults(), NULL ).openAllNeoStores( true );
        MetaDataStore metaDataStore = neoStores.getMetaDataStore();

        // Then
        assertThat( metaDataStore.getUpgradeTime() ).isEqualTo( metaDataStore.getCreationTime() );
    }

    @Test
    void shouldHaveSameCommittedTransactionAndUpgradeTransactionOnStartup()
    {
        // When
        neoStores = storeFactory( defaults(), NULL ).openAllNeoStores( true );
        MetaDataStore metaDataStore = neoStores.getMetaDataStore();

        // Then
        assertEquals( metaDataStore.getUpgradeTransaction(), metaDataStore.getLastCommittedTransaction() );
    }

    @Test
    void shouldThrowWhenOpeningNonExistingNeoStores()
    {
        assertThrows( StoreNotFoundException.class, () ->
        {
            try ( NeoStores neoStores = storeFactory( defaults(), NULL ).openAllNeoStores() )
            {
                neoStores.getMetaDataStore();
            }
        } );
    }

    @Test
    void shouldDelegateDeletionOptionToStores()
    {
        // GIVEN
        StoreFactory storeFactory = storeFactory( defaults(), NULL, immutable.of( DELETE_ON_CLOSE ) );

        // WHEN
        neoStores = storeFactory.openAllNeoStores( true );
        assertTrue( fileSystem.listFiles( databaseLayout.databaseDirectory().toFile() ).length >= StoreType.values().length );

        // THEN
        neoStores.close();
        assertEquals( 0, fileSystem.listFiles( databaseLayout.databaseDirectory().toFile() ).length );
    }

    @Test
    void shouldHandleStoreConsistingOfOneEmptyFile() throws Exception
    {
        StoreFactory storeFactory = storeFactory( defaults(), NULL );
        fileSystem.write( databaseLayout.file( "neostore.nodestore.db.labels" ).toFile() );
        storeFactory.openAllNeoStores( true ).close();
    }

    @Test
    void shouldCompleteInitializationOfStoresWithIncompleteHeaders() throws Exception
    {
        StoreFactory storeFactory = storeFactory( defaults(), NULL );
        storeFactory.openAllNeoStores( true ).close();
        for ( File f : fileSystem.listFiles( databaseLayout.databaseDirectory().toFile() ) )
        {
            if ( !f.getName().endsWith( ".id" ) )
            {
                fileSystem.truncate( f, 0 );
            }
        }
        storeFactory = storeFactory( defaults(), NULL );
        storeFactory.openAllNeoStores( true ).close();
    }
}
