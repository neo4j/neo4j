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
package org.neo4j.kernel.impl.index.schema;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.index.internal.gbptree.TreeNodeDynamicSize;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProviderDescriptor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsFactory;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.schema.SimpleNodeValueClient;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.internal.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexProvider.Monitor.EMPTY;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.kernel.impl.index.schema.IndexDescriptorFactory.forSchema;
import static org.neo4j.kernel.impl.index.schema.IndexDescriptorFactory.uniqueForSchema;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;
import static org.neo4j.values.storable.Values.stringValue;

public class GenericBlockBasedIndexPopulatorTest
{
    private static final StoreIndexDescriptor INDEX_DESCRIPTOR = forSchema( forLabel( 1, 1 ) ).withId( 1 );
    private static final StoreIndexDescriptor UNIQUE_INDEX_DESCRIPTOR = uniqueForSchema( forLabel( 1, 1 ) ).withId( 1 );

    private FileSystemAbstraction fs;
    private IndexFiles indexFiles;

    @Before
    public void setup()
    {
        fs = storage.fileSystem();
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor( "test", "v1" );
        IndexDirectoryStructure directoryStructure = directoriesByProvider( storage.directory().storeDir() ).forProvider( providerDescriptor );
        indexFiles = new IndexFiles.Directory( fs, directoryStructure, INDEX_DESCRIPTOR.getId() );
    }

    @Rule
    public final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule();

    @Test
    public void shouldSeeExternalUpdateBothBeforeAndAfterScanCompleted() throws IndexEntryConflictException
    {
        // given
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( INDEX_DESCRIPTOR );
        try
        {
            // when
            TextValue hakuna = stringValue( "hakuna" );
            TextValue matata = stringValue( "matata" );
            int hakunaId = 1;
            int matataId = 2;
            externalUpdate( populator, hakuna, hakunaId );
            populator.scanCompleted( nullInstance );
            externalUpdate( populator, matata, matataId );

            // then
            assertMatch( populator, hakuna, hakunaId );
            assertMatch( populator, matata, matataId );
        }
        finally
        {
            populator.close( true );
        }
    }

    @Test
    public void shouldThrowOnDuplicatedValuesFromScan()
    {
        // given
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( UNIQUE_INDEX_DESCRIPTOR );
        try
        {
            // when
            Value duplicate = Values.of( "duplicate" );
            IndexEntryUpdate<?> firstScanUpdate = IndexEntryUpdate.add( 1, INDEX_DESCRIPTOR, duplicate );
            IndexEntryUpdate<?> secondScanUpdate = IndexEntryUpdate.add( 2, INDEX_DESCRIPTOR, duplicate );
            try
            {
                populator.add( singleton( firstScanUpdate ) );
                populator.add( singleton( secondScanUpdate ) );
                populator.scanCompleted( nullInstance );

                fail( "Expected to throw" );
            }
            catch ( IndexEntryConflictException e )
            {
                // then
            }
        }
        finally
        {
            populator.close( true );
        }
    }

    @Test
    public void shouldThrowOnDuplicatedValuesFromExternalUpdates()
    {
        // given
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( UNIQUE_INDEX_DESCRIPTOR );
        try
        {
            // when
            Value duplicate = Values.of( "duplicate" );
            IndexEntryUpdate<?> firstExternalUpdate = IndexEntryUpdate.add( 1, INDEX_DESCRIPTOR, duplicate );
            IndexEntryUpdate<?> secondExternalUpdate = IndexEntryUpdate.add( 2, INDEX_DESCRIPTOR, duplicate );
            try
            {
                try ( IndexUpdater updater = populator.newPopulatingUpdater() )
                {
                    updater.process( firstExternalUpdate );
                    updater.process( secondExternalUpdate );
                }
                populator.scanCompleted( nullInstance );

                fail( "Expected to throw" );
            }
            catch ( IndexEntryConflictException e )
            {
                // then
            }
        }
        finally
        {
            populator.close( true );
        }
    }

    @Test
    public void shouldThrowOnDuplicatedValuesFromScanAndExternalUpdates()
    {
        // given
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( UNIQUE_INDEX_DESCRIPTOR );
        try
        {
            // when
            Value duplicate = Values.of( "duplicate" );
            IndexEntryUpdate<?> externalUpdate = IndexEntryUpdate.add( 1, INDEX_DESCRIPTOR, duplicate );
            IndexEntryUpdate<?> scanUpdate = IndexEntryUpdate.add( 2, INDEX_DESCRIPTOR, duplicate );
            try
            {
                try ( IndexUpdater updater = populator.newPopulatingUpdater() )
                {
                    updater.process( externalUpdate );
                }
                populator.add( singleton( scanUpdate ) );
                populator.scanCompleted( nullInstance );

                fail( "Expected to throw" );
            }
            catch ( IndexEntryConflictException e )
            {
                // then
            }
        }
        finally
        {
            populator.close( true );
        }
    }

    @Test
    public void shouldNotThrowOnDuplicationsLaterFixedByExternalUpdates() throws IndexEntryConflictException
    {
        // given
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( UNIQUE_INDEX_DESCRIPTOR );
        try
        {
            // when
            Value duplicate = Values.of( "duplicate" );
            Value unique = Values.of( "unique" );
            IndexEntryUpdate<?> firstScanUpdate = IndexEntryUpdate.add( 1, INDEX_DESCRIPTOR, duplicate );
            IndexEntryUpdate<?> secondScanUpdate = IndexEntryUpdate.add( 2, INDEX_DESCRIPTOR, duplicate );
            IndexEntryUpdate<?> externalUpdate = IndexEntryUpdate.change( 1, INDEX_DESCRIPTOR, duplicate, unique );
            populator.add( singleton( firstScanUpdate ) );
            try ( IndexUpdater updater = populator.newPopulatingUpdater() )
            {
                updater.process( externalUpdate );
            }
            populator.add( singleton( secondScanUpdate ) );
            populator.scanCompleted( nullInstance );

            // then
            assertHasEntry( populator, unique, 1 );
            assertHasEntry( populator, duplicate, 2 );
        }
        finally
        {
            populator.close( true );
        }
    }

    @Test
    public void shouldHandleEntriesOfMaxSize() throws IndexEntryConflictException
    {
        // given
        int sizeOfEntityId = GenericKey.ENTITY_ID_SIZE;
        int sizeOfType = GenericKey.TYPE_ID_SIZE;
        int sizeOfStringLength = GenericKey.SIZE_STRING_LENGTH;
        int keySizeLimit = TreeNodeDynamicSize.keyValueSizeCapFromPageSize( PageCache.PAGE_SIZE );
        int stringKeyOverhead = sizeOfEntityId + sizeOfType + sizeOfStringLength;

        String largestString = RandomStringUtils.randomAlphabetic( keySizeLimit - stringKeyOverhead );
        TextValue largestStringValue = stringValue( largestString );
        IndexEntryUpdate<StoreIndexDescriptor> update = add( 1, INDEX_DESCRIPTOR, largestStringValue );

        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( INDEX_DESCRIPTOR );
        try
        {
            // when
            Collection<IndexEntryUpdate<?>> updates = Collections.singleton( update );
            populator.add( updates );
            populator.scanCompleted( nullInstance );

            // then
            assertHasEntry( populator, largestStringValue, 1 );
        }
        finally
        {
            populator.close( true );
        }
    }

    @Test
    public void shouldThrowForEntriesLargerThanMaxSize() throws IndexEntryConflictException
    {
        // given
        int sizeOfEntityId = GenericKey.ENTITY_ID_SIZE;
        int sizeOfType = GenericKey.TYPE_ID_SIZE;
        int sizeOfStringLength = GenericKey.SIZE_STRING_LENGTH;
        int keySizeLimit = TreeNodeDynamicSize.keyValueSizeCapFromPageSize( PageCache.PAGE_SIZE );
        int stringKeyOverhead = sizeOfEntityId + sizeOfType + sizeOfStringLength;

        String largestString = RandomStringUtils.randomAlphabetic( keySizeLimit - stringKeyOverhead + 1 );
        TextValue largestStringValue = stringValue( largestString );
        IndexEntryUpdate<StoreIndexDescriptor> update = add( 1, INDEX_DESCRIPTOR, largestStringValue );

        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator( INDEX_DESCRIPTOR );
        try
        {
            // when
            Collection<IndexEntryUpdate<?>> updates = Collections.singleton( update );
            populator.add( updates );
            populator.scanCompleted( nullInstance );

            // if not
            fail( "Expected to throw for value larger than max size." );
        }
        catch ( IllegalArgumentException e )
        {
            // then good
        }
        finally
        {
            populator.close( true );
        }
    }

    private void assertHasEntry( BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator, Value entry, int expectedId )
    {
        try ( NativeIndexReader<GenericKey,NativeIndexValue> reader = populator.newReader() )
        {
            SimpleNodeValueClient valueClient = new SimpleNodeValueClient();
            IndexQuery.ExactPredicate exact = IndexQuery.exact( INDEX_DESCRIPTOR.properties()[0], entry );
            reader.query( QueryContext.NULL_CONTEXT, valueClient, IndexOrder.NONE, false, exact );
            assertTrue( valueClient.next() );
            long id = valueClient.reference;
            assertEquals( expectedId, id );
        }
    }

    private void externalUpdate( BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator, TextValue matata, int matataId )
            throws IndexEntryConflictException
    {
        try ( IndexUpdater indexUpdater = populator.newPopulatingUpdater() )
        {
            // After scanCompleted
            indexUpdater.process( add( matataId, INDEX_DESCRIPTOR, matata ) );
        }
    }

    private void assertMatch( BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator, Value value, long id )
    {
        try ( NativeIndexReader<GenericKey,NativeIndexValue> reader = populator.newReader() )
        {
            SimpleNodeValueClient cursor = new SimpleNodeValueClient();
            reader.query( NULL_CONTEXT, cursor, IndexOrder.NONE, true, IndexQuery.exact( INDEX_DESCRIPTOR.properties()[0], value ) );
            assertTrue( cursor.next() );
            assertEquals( id, cursor.reference );
            assertEquals( value, cursor.values[0] );
            assertFalse( cursor.next() );
        }
    }

    private GenericBlockBasedIndexPopulator instantiatePopulator( StoreIndexDescriptor indexDescriptor )
    {
        Config config = Config.defaults();
        ConfiguredSpaceFillingCurveSettingsCache settingsCache = new ConfiguredSpaceFillingCurveSettingsCache( config );
        IndexSpecificSpaceFillingCurveSettingsCache spatialSettings = new IndexSpecificSpaceFillingCurveSettingsCache( settingsCache, new HashMap<>() );
        GenericLayout layout = new GenericLayout( 1, spatialSettings );
        SpaceFillingCurveConfiguration configuration = SpaceFillingCurveSettingsFactory.getConfiguredSpaceFillingCurveConfiguration( config );
        PageCache pc = storage.pageCache();
        GenericBlockBasedIndexPopulator populator =
                new GenericBlockBasedIndexPopulator( pc, fs, indexFiles, layout, EMPTY, indexDescriptor, spatialSettings, configuration, false );
        populator.create();
        return populator;
    }
}
