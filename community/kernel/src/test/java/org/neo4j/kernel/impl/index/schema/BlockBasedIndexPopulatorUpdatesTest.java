/*
 * Copyright (c) "Neo4j"
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

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.storageengine.api.schema.SimpleEntityValueClient;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unorderedValues;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.SIMPLE_NAME_LOOKUP;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.kernel.impl.index.schema.IndexEntryTestUtil.generateStringValueResultingInIndexEntrySize;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;
import static org.neo4j.values.storable.Values.stringValue;

@PageCacheExtension
abstract class BlockBasedIndexPopulatorUpdatesTest<KEY extends NativeIndexKey<KEY>>
{
    private final IndexDescriptor INDEX_DESCRIPTOR = forSchema( forLabel( 1, 1 ) ).withName( "index" ).withIndexType( indexType() ).materialise( 1 );
    private final IndexDescriptor UNIQUE_INDEX_DESCRIPTOR =
            uniqueForSchema( forLabel( 1, 1 ) ).withName( "constraint" ).withIndexType( indexType() ).materialise( 1 );
    final TokenNameLookup tokenNameLookup = SIMPLE_NAME_LOOKUP;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory directory;
    @Inject
    private PageCache pageCache;

    IndexFiles indexFiles;
    DatabaseIndexContext databaseIndexContext;
    private JobScheduler jobScheduler;
    private IndexPopulator.PopulationWorkScheduler populationWorkScheduler;

    abstract IndexType indexType();
    abstract BlockBasedIndexPopulator<KEY> instantiatePopulator( IndexDescriptor indexDescriptor ) throws IOException;

    @BeforeEach
    void setup()
    {
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor( "test", "v1" );
        IndexDirectoryStructure directoryStructure = directoriesByProvider( directory.homePath() ).forProvider( providerDescriptor );
        indexFiles = new IndexFiles.Directory( fs, directoryStructure, INDEX_DESCRIPTOR.getId() );
        databaseIndexContext = DatabaseIndexContext.builder( pageCache, fs, DEFAULT_DATABASE_NAME ).build();
        jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        populationWorkScheduler = new IndexPopulator.PopulationWorkScheduler()
        {

            @Override
            public <T> JobHandle<T> schedule( IndexPopulator.JobDescriptionSupplier descriptionSupplier, Callable<T> job )
            {
                return jobScheduler.schedule( Group.INDEX_POPULATION_WORK, new JobMonitoringParams( null, null, null ), job );
            }
        };
    }

    @AfterEach
    void tearDown() throws Exception
    {
        jobScheduler.shutdown();
    }

    @Test
    void shouldSeeExternalUpdateBothBeforeAndAfterScanCompleted() throws IndexEntryConflictException, IOException
    {
        // given
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator( INDEX_DESCRIPTOR );
        try
        {
            // when
            TextValue hakuna = stringValue( "hakuna" );
            TextValue matata = stringValue( "matata" );
            int hakunaId = 1;
            int matataId = 2;
            externalUpdate( populator, hakuna, hakunaId );
            populator.scanCompleted( nullInstance, populationWorkScheduler, NULL );
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
    void shouldThrowOnDuplicatedValuesFromScan() throws IOException
    {
        // given
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator( UNIQUE_INDEX_DESCRIPTOR );
        try
        {
            // when
            Value duplicate = Values.of( "duplicate" );
            ValueIndexEntryUpdate<?> firstScanUpdate = ValueIndexEntryUpdate.add( 1, INDEX_DESCRIPTOR, duplicate );
            ValueIndexEntryUpdate<?> secondScanUpdate = ValueIndexEntryUpdate.add( 2, INDEX_DESCRIPTOR, duplicate );
            assertThrows( IndexEntryConflictException.class, () ->
            {
                populator.add( singleton( firstScanUpdate ), NULL );
                populator.add( singleton( secondScanUpdate ), NULL );
                populator.scanCompleted( nullInstance, populationWorkScheduler, NULL );
            } );
        }
        finally
        {
            populator.close( true, NULL );
        }
    }

    @Test
    void shouldThrowOnDuplicatedValuesFromExternalUpdates() throws IOException
    {
        // given
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator( UNIQUE_INDEX_DESCRIPTOR );
        try
        {
            // when
            Value duplicate = Values.of( "duplicate" );
            ValueIndexEntryUpdate<?> firstExternalUpdate = ValueIndexEntryUpdate.add( 1, INDEX_DESCRIPTOR, duplicate );
            ValueIndexEntryUpdate<?> secondExternalUpdate = ValueIndexEntryUpdate.add( 2, INDEX_DESCRIPTOR, duplicate );
            assertThrows( IndexEntryConflictException.class, () ->
            {
                try ( IndexUpdater updater = populator.newPopulatingUpdater( NULL ) )
                {
                    updater.process( firstExternalUpdate );
                    updater.process( secondExternalUpdate );
                }
                populator.scanCompleted( nullInstance, populationWorkScheduler, NULL );
            } );
        }
        finally
        {
            populator.close( true, NULL );
        }
    }

    @Test
    void shouldThrowOnDuplicatedValuesFromScanAndExternalUpdates() throws IOException
    {
        // given
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator( UNIQUE_INDEX_DESCRIPTOR );
        try
        {
            // when
            Value duplicate = Values.of( "duplicate" );
            ValueIndexEntryUpdate<?> externalUpdate = ValueIndexEntryUpdate.add( 1, INDEX_DESCRIPTOR, duplicate );
            ValueIndexEntryUpdate<?> scanUpdate = ValueIndexEntryUpdate.add( 2, INDEX_DESCRIPTOR, duplicate );
            assertThrows( IndexEntryConflictException.class, () ->
            {
                try ( IndexUpdater updater = populator.newPopulatingUpdater( NULL ) )
                {
                    updater.process( externalUpdate );
                }
                populator.add( singleton( scanUpdate ), NULL );
                populator.scanCompleted( nullInstance, populationWorkScheduler, NULL );
            } );
        }
        finally
        {
            populator.close( true, NULL );
        }
    }

    @Test
    void shouldNotThrowOnDuplicationsLaterFixedByExternalUpdates() throws IndexEntryConflictException, IOException
    {
        // given
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator( UNIQUE_INDEX_DESCRIPTOR );
        try
        {
            // when
            Value duplicate = Values.of( "duplicate" );
            Value unique = Values.of( "unique" );
            ValueIndexEntryUpdate<?> firstScanUpdate = ValueIndexEntryUpdate.add( 1, INDEX_DESCRIPTOR, duplicate );
            ValueIndexEntryUpdate<?> secondScanUpdate = ValueIndexEntryUpdate.add( 2, INDEX_DESCRIPTOR, duplicate );
            ValueIndexEntryUpdate<?> externalUpdate = ValueIndexEntryUpdate.change( 1, INDEX_DESCRIPTOR, duplicate, unique );
            populator.add( singleton( firstScanUpdate ), NULL );
            try ( IndexUpdater updater = populator.newPopulatingUpdater( NULL ) )
            {
                updater.process( externalUpdate );
            }
            populator.add( singleton( secondScanUpdate ), NULL );
            populator.scanCompleted( nullInstance, populationWorkScheduler, NULL );

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
    void shouldHandleEntriesOfMaxSize() throws IndexEntryConflictException, IOException
    {
        // given
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator( INDEX_DESCRIPTOR );
        try
        {
            int maxKeyValueSize = populator.tree.keyValueSizeCap();
            ValueIndexEntryUpdate<IndexDescriptor> update =
                    add( 1, INDEX_DESCRIPTOR, generateStringValueResultingInIndexEntrySize( populator.layout, maxKeyValueSize ) );

            // when
            Collection<ValueIndexEntryUpdate<?>> updates = singleton( update );
            populator.add( updates, NULL );
            populator.scanCompleted( nullInstance, populationWorkScheduler, NULL );

            // then
            assertHasEntry( populator, update.values()[0], 1 );
        }
        finally
        {
            populator.close( true, NULL );
        }
    }

    @Test
    void shouldThrowForEntriesLargerThanMaxSize() throws IOException
    {
        // given
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator( INDEX_DESCRIPTOR );
        try
        {
            int maxKeyValueSize = populator.tree.keyValueSizeCap();
            ValueIndexEntryUpdate<IndexDescriptor> update =
                    add( 1, INDEX_DESCRIPTOR, generateStringValueResultingInIndexEntrySize( populator.layout, maxKeyValueSize + 1 ) );
            IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () ->
            {
                Collection<ValueIndexEntryUpdate<?>> updates = singleton( update );
                populator.add( updates, NULL );
                populator.scanCompleted( nullInstance, populationWorkScheduler, NULL );
            } );
            // then
            assertThat( e.getMessage(), Matchers.containsString(
                    "Property value is too large to index, please see index documentation for limitations. Index: Index( id=1, name='index', " +
                            "type='GENERAL " + indexType() + "', schema=(:Label1 {property1}), indexProvider='Undecided-0' ), entity id: 1, property size: " +
                            "8176, value: [String(\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...." ) );

        }
        finally
        {
            populator.close( true, NULL );
        }
    }

    private void assertHasEntry( BlockBasedIndexPopulator<KEY> populator, Value entry, int expectedId )
    {
        try ( NativeIndexReader<KEY> reader = populator.newReader() )
        {
            SimpleEntityValueClient valueClient = new SimpleEntityValueClient();
            PropertyIndexQuery.ExactPredicate exact = PropertyIndexQuery.exact( INDEX_DESCRIPTOR.schema().getPropertyId(), entry );
            reader.query( valueClient, NULL_CONTEXT, AccessMode.Static.READ, unconstrained(), exact );
            assertTrue( valueClient.next() );
            long id = valueClient.reference;
            assertEquals( expectedId, id );
        }
    }

    private void externalUpdate( BlockBasedIndexPopulator<KEY> populator, TextValue matata, int matataId )
        throws IndexEntryConflictException
    {
        try ( IndexUpdater indexUpdater = populator.newPopulatingUpdater( NULL ) )
        {
            // After scanCompleted
            indexUpdater.process( add( matataId, INDEX_DESCRIPTOR, matata ) );
        }
    }

    private void assertMatch( BlockBasedIndexPopulator<KEY> populator, Value value, long id )
    {
        try ( NativeIndexReader<KEY> reader = populator.newReader() )
        {
            SimpleEntityValueClient cursor = new SimpleEntityValueClient();
            reader.query( cursor, NULL_CONTEXT, AccessMode.Static.READ, unorderedValues(),
                          PropertyIndexQuery.exact( INDEX_DESCRIPTOR.schema().getPropertyId(), value ) );
            assertTrue( cursor.next() );
            assertEquals( id, cursor.reference );
            assertEquals( value, cursor.values[0] );
            assertFalse( cursor.next() );
        }
    }
}
