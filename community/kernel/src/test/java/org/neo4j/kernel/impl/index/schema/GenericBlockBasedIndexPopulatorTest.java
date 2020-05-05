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
package org.neo4j.kernel.impl.index.schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.schema.SimpleNodeValueClient;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unorderedValues;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;
import static org.neo4j.values.storable.Values.stringValue;

@PageCacheExtension
class GenericBlockBasedIndexPopulatorTest
{
    private static final IndexDescriptor INDEX_DESCRIPTOR = forSchema( forLabel( 1, 1 ) ).withName( "index" ).materialise( 1 );
    private static final IndexDescriptor UNIQUE_INDEX_DESCRIPTOR = uniqueForSchema( forLabel( 1, 1 ) ).withName( "constrain" ).materialise( 1 );

    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory directory;
    @Inject
    private PageCache pageCache;

    private IndexFiles indexFiles;
    private DatabaseIndexContext databaseIndexContext;
    private JobScheduler jobScheduler;

    @BeforeEach
    void setup()
    {
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor( "test", "v1" );
        IndexDirectoryStructure directoryStructure = directoriesByProvider( directory.homeDir() ).forProvider( providerDescriptor );
        indexFiles = new IndexFiles( fs, directoryStructure, INDEX_DESCRIPTOR.getId() );
        databaseIndexContext = DatabaseIndexContext.builder( pageCache, fs ).build();
        jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
    }

    @AfterEach
    void tearDown() throws Exception
    {
        jobScheduler.shutdown();
    }

    @Test
    void shouldSeeExternalUpdateBothBeforeAndAfterScanCompleted() throws IndexEntryConflictException
    {
        // given
        BlockBasedIndexPopulator<GenericKey, NativeIndexValue> populator = instantiatePopulator( INDEX_DESCRIPTOR );
        try
        {
            // when
            TextValue hakuna = stringValue( "hakuna" );
            TextValue matata = stringValue( "matata" );
            int hakunaId = 1;
            int matataId = 2;
            externalUpdate( populator, hakuna, hakunaId );
            populator.scanCompleted( nullInstance, jobScheduler, NULL );
            externalUpdate( populator, matata, matataId );

            // then
            assertMatch( populator, hakuna, hakunaId );
            assertMatch( populator, matata, matataId );
        }
        finally
        {
            populator.close( true, NULL );
        }
    }

    @Test
    void shouldThrowOnDuplicatedValuesFromScan()
    {
        // given
        BlockBasedIndexPopulator<GenericKey, NativeIndexValue> populator = instantiatePopulator( UNIQUE_INDEX_DESCRIPTOR );
        try
        {
            // when
            Value duplicate = Values.of( "duplicate" );
            IndexEntryUpdate<?> firstScanUpdate = IndexEntryUpdate.add( 1, INDEX_DESCRIPTOR, duplicate );
            IndexEntryUpdate<?> secondScanUpdate = IndexEntryUpdate.add( 2, INDEX_DESCRIPTOR, duplicate );
            assertThrows( IndexEntryConflictException.class, () ->
            {
                populator.add( singleton( firstScanUpdate ), NULL );
                populator.add( singleton( secondScanUpdate ), NULL );
                populator.scanCompleted( nullInstance, jobScheduler, NULL );
            } );
        }
        finally
        {
            populator.close( true, NULL );
        }
    }

    @Test
    void shouldThrowOnDuplicatedValuesFromExternalUpdates()
    {
        // given
        BlockBasedIndexPopulator<GenericKey, NativeIndexValue> populator = instantiatePopulator( UNIQUE_INDEX_DESCRIPTOR );
        try
        {
            // when
            Value duplicate = Values.of( "duplicate" );
            IndexEntryUpdate<?> firstExternalUpdate = IndexEntryUpdate.add( 1, INDEX_DESCRIPTOR, duplicate );
            IndexEntryUpdate<?> secondExternalUpdate = IndexEntryUpdate.add( 2, INDEX_DESCRIPTOR, duplicate );
            assertThrows( IndexEntryConflictException.class, () ->
            {
                try ( IndexUpdater updater = populator.newPopulatingUpdater( NULL ) )
                {
                    updater.process( firstExternalUpdate );
                    updater.process( secondExternalUpdate );
                }
                populator.scanCompleted( nullInstance, jobScheduler, NULL );
            } );
        }
        finally
        {
            populator.close( true, NULL );
        }
    }

    @Test
    void shouldThrowOnDuplicatedValuesFromScanAndExternalUpdates()
    {
        // given
        BlockBasedIndexPopulator<GenericKey, NativeIndexValue> populator = instantiatePopulator( UNIQUE_INDEX_DESCRIPTOR );
        try
        {
            // when
            Value duplicate = Values.of( "duplicate" );
            IndexEntryUpdate<?> externalUpdate = IndexEntryUpdate.add( 1, INDEX_DESCRIPTOR, duplicate );
            IndexEntryUpdate<?> scanUpdate = IndexEntryUpdate.add( 2, INDEX_DESCRIPTOR, duplicate );
            assertThrows( IndexEntryConflictException.class, () ->
            {
                try ( IndexUpdater updater = populator.newPopulatingUpdater( NULL ) )
                {
                    updater.process( externalUpdate );
                }
                populator.add( singleton( scanUpdate ), NULL );
                populator.scanCompleted( nullInstance, jobScheduler, NULL );
            } );
        }
        finally
        {
            populator.close( true, NULL );
        }
    }

    @Test
    void shouldNotThrowOnDuplicationsLaterFixedByExternalUpdates() throws IndexEntryConflictException
    {
        // given
        BlockBasedIndexPopulator<GenericKey, NativeIndexValue> populator = instantiatePopulator( UNIQUE_INDEX_DESCRIPTOR );
        try
        {
            // when
            Value duplicate = Values.of( "duplicate" );
            Value unique = Values.of( "unique" );
            IndexEntryUpdate<?> firstScanUpdate = IndexEntryUpdate.add( 1, INDEX_DESCRIPTOR, duplicate );
            IndexEntryUpdate<?> secondScanUpdate = IndexEntryUpdate.add( 2, INDEX_DESCRIPTOR, duplicate );
            IndexEntryUpdate<?> externalUpdate = IndexEntryUpdate.change( 1, INDEX_DESCRIPTOR, duplicate, unique );
            populator.add( singleton( firstScanUpdate ), NULL );
            try ( IndexUpdater updater = populator.newPopulatingUpdater( NULL ) )
            {
                updater.process( externalUpdate );
            }
            populator.add( singleton( secondScanUpdate ), NULL );
            populator.scanCompleted( nullInstance, jobScheduler, NULL );

            // then
            assertHasEntry( populator, unique, 1 );
            assertHasEntry( populator, duplicate, 2 );
        }
        finally
        {
            populator.close( true, NULL );
        }
    }

    @Test
    void shouldHandleEntriesOfMaxSize() throws IndexEntryConflictException
    {
        // given
        BlockBasedIndexPopulator<GenericKey, NativeIndexValue> populator = instantiatePopulator( INDEX_DESCRIPTOR );
        try
        {
            int maxKeyValueSize = populator.tree.keyValueSizeCap();
            IndexEntryUpdate<IndexDescriptor> update =
                    add( 1, INDEX_DESCRIPTOR, BlockBasedIndexPopulatorTest.generateStringResultingInSize( populator.layout, maxKeyValueSize ) );

            // when
            Collection<IndexEntryUpdate<?>> updates = singleton( update );
            populator.add( updates, NULL );
            populator.scanCompleted( nullInstance, jobScheduler, NULL );

            // then
            assertHasEntry( populator, update.values()[0], 1 );
        }
        finally
        {
            populator.close( true, NULL );
        }
    }

    @Test
    void shouldThrowForEntriesLargerThanMaxSize()
    {
        // given
        BlockBasedIndexPopulator<GenericKey, NativeIndexValue> populator = instantiatePopulator( INDEX_DESCRIPTOR );
        try
        {
            int maxKeyValueSize = populator.tree.keyValueSizeCap();
            IndexEntryUpdate<IndexDescriptor> update =
                    add( 1, INDEX_DESCRIPTOR, BlockBasedIndexPopulatorTest.generateStringResultingInSize( populator.layout, maxKeyValueSize + 1 ) );
            assertThrows( IllegalArgumentException.class, () ->
            {
                Collection<IndexEntryUpdate<?>> updates = singleton( update );
                populator.add( updates, NULL );
                populator.scanCompleted( nullInstance, jobScheduler, NULL );

                // if not
                fail( "Expected to throw for value larger than max size." );
            } );
        }
        finally
        {
            populator.close( true, NULL );
        }
    }

    private static void assertHasEntry( BlockBasedIndexPopulator<GenericKey, NativeIndexValue> populator, Value entry, int expectedId )
    {
        try ( NativeIndexReader<GenericKey, NativeIndexValue> reader = populator.newReader() )
        {
            SimpleNodeValueClient valueClient = new SimpleNodeValueClient();
            IndexQuery.ExactPredicate exact = IndexQuery.exact( INDEX_DESCRIPTOR.schema().getPropertyId(), entry );
            reader.query( NULL_CONTEXT, valueClient, unconstrained(), exact );
            assertTrue( valueClient.next() );
            long id = valueClient.reference;
            assertEquals( expectedId, id );
        }
    }

    private static void externalUpdate( BlockBasedIndexPopulator<GenericKey, NativeIndexValue> populator, TextValue matata, int matataId )
        throws IndexEntryConflictException
    {
        try ( IndexUpdater indexUpdater = populator.newPopulatingUpdater( NULL ) )
        {
            // After scanCompleted
            indexUpdater.process( add( matataId, INDEX_DESCRIPTOR, matata ) );
        }
    }

    private static void assertMatch( BlockBasedIndexPopulator<GenericKey, NativeIndexValue> populator, Value value, long id )
    {
        try ( NativeIndexReader<GenericKey, NativeIndexValue> reader = populator.newReader() )
        {
            SimpleNodeValueClient cursor = new SimpleNodeValueClient();
            reader.query( NULL_CONTEXT, cursor, unorderedValues(), IndexQuery.exact( INDEX_DESCRIPTOR.schema().getPropertyId(), value ) );
            assertTrue( cursor.next() );
            assertEquals( id, cursor.reference );
            assertEquals( value, cursor.values[0] );
            assertFalse( cursor.next() );
        }
    }

    private GenericBlockBasedIndexPopulator instantiatePopulator( IndexDescriptor indexDescriptor )
    {
        Config config = Config.defaults();
        IndexSpecificSpaceFillingCurveSettings spatialSettings = IndexSpecificSpaceFillingCurveSettings.fromConfig( config );
        GenericLayout layout = new GenericLayout( 1, spatialSettings );
        SpaceFillingCurveConfiguration configuration = SpaceFillingCurveSettingsFactory.getConfiguredSpaceFillingCurveConfiguration( config );
        GenericBlockBasedIndexPopulator populator =
                new GenericBlockBasedIndexPopulator( databaseIndexContext, indexFiles, layout, indexDescriptor, spatialSettings, configuration, false,
                heapBufferFactory( (int) kibiBytes( 40 ) ), INSTANCE );
        populator.create();
        return populator;
    }
}
