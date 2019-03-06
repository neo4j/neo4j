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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsFactory;
import org.neo4j.storageengine.api.schema.IndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.SimpleNodeValueClient;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.add;
import static org.neo4j.kernel.api.index.IndexProvider.Monitor.EMPTY;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.values.storable.Values.stringValue;

public class GenericBlockBasedIndexPopulatorTest
{
    private static final StoreIndexDescriptor INDEX_DESCRIPTOR = IndexDescriptorFactory.forSchema( SchemaDescriptorFactory.forLabel( 1, 1 ) ).withId( 1 );

    private IndexDirectoryStructure directoryStructure;
    private File indexFile;
    private FileSystemAbstraction fs;
    private IndexDropAction dropAction;

    @Before
    public void setup()
    {
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor( "test", "v1" );
        directoryStructure = directoriesByProvider( storage.directory().databaseDir() ).forProvider( providerDescriptor );
        File indexDir = directoryStructure.directoryForIndex( INDEX_DESCRIPTOR.getId() );
        indexFile = new File( indexDir, "index" );
        fs = storage.fileSystem();
        dropAction = new FileSystemIndexDropAction( fs, directoryStructure );
    }

    @Rule
    public final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule();

    @Test
    public void shouldSeeExternalUpdateBothBeforeAndAfterScanCompleted() throws IndexEntryConflictException
    {
        // given
        BlockBasedIndexPopulator<GenericKey,NativeIndexValue> populator = instantiatePopulator();
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
            reader.query( cursor, IndexOrder.NONE, true, IndexQuery.exact( INDEX_DESCRIPTOR.properties()[0], value ) );
            assertTrue( cursor.next() );
            assertEquals( id, cursor.reference );
            assertEquals( value, cursor.values[0] );
            assertFalse( cursor.next() );
        }
    }

    private GenericBlockBasedIndexPopulator instantiatePopulator()
    {
        Config config = Config.defaults();
        ConfiguredSpaceFillingCurveSettingsCache settingsCache = new ConfiguredSpaceFillingCurveSettingsCache( config );
        IndexSpecificSpaceFillingCurveSettingsCache spatialSettings = new IndexSpecificSpaceFillingCurveSettingsCache( settingsCache, new HashMap<>() );
        GenericLayout layout = new GenericLayout( 1, spatialSettings );
        SpaceFillingCurveConfiguration configuration =
                SpaceFillingCurveSettingsFactory.getConfiguredSpaceFillingCurveConfiguration( config );
        GenericBlockBasedIndexPopulator populator =
                new GenericBlockBasedIndexPopulator( storage.pageCache(), fs, indexFile, layout, EMPTY, INDEX_DESCRIPTOR, spatialSettings, directoryStructure,
                        configuration, dropAction, false );
        populator.create();
        return populator;
    }
}
