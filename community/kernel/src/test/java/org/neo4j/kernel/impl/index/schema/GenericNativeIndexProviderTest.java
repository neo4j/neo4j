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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.kernel.impl.index.schema.SpatialIndexConfig.CODE;
import static org.neo4j.kernel.impl.index.schema.SpatialIndexConfig.DIMENSIONS;
import static org.neo4j.kernel.impl.index.schema.SpatialIndexConfig.MAX;
import static org.neo4j.kernel.impl.index.schema.SpatialIndexConfig.MAX_LEVELS;
import static org.neo4j.kernel.impl.index.schema.SpatialIndexConfig.MIN;
import static org.neo4j.kernel.impl.index.schema.SpatialIndexConfig.TABLE_ID;
import static org.neo4j.kernel.impl.index.schema.SpatialIndexConfig.key;

class GenericNativeIndexProviderTest
{
    @Test
    void mustCompleteIndexDescriptorConfigurationsWithSpatialConfig()
    {
        // Given
        GenericNativeIndexProvider provider = new GenericNativeIndexProvider( IndexDirectoryStructure.NONE, null, null, null, null, false, Config.defaults() );
        LabelSchemaDescriptor incompleteSchema = SchemaDescriptor.forLabel( 1, 1 );
        IndexDescriptor incompleteDescriptor = IndexPrototype.forSchema( incompleteSchema, IndexProviderDescriptor.UNDECIDED )
                .withName( "index" ).materialise( 1 );

        // When
        IndexDescriptor completedDescriptor = provider.completeConfiguration( incompleteDescriptor );
        SchemaDescriptor completedSchema = completedDescriptor.schema();

        // Then
        IndexConfig sinfulIndexConfig = incompleteSchema.getIndexConfig();
        IndexConfig completedIndexConfig = completedSchema.getIndexConfig();
        assertEquals( 0, sinfulIndexConfig.entries().count( p -> true ), "expected sinful index config to have no entries" );
        for ( CoordinateReferenceSystem crs : CoordinateReferenceSystem.all() )
        {
            assertNotNull( completedIndexConfig.get( key( crs.getName(), TABLE_ID ) ) );
            assertNotNull( completedIndexConfig.get( key( crs.getName(), CODE ) ) );
            assertNotNull( completedIndexConfig.get( key( crs.getName(), DIMENSIONS ) ) );
            assertNotNull( completedIndexConfig.get( key( crs.getName(), MAX_LEVELS ) ) );
            assertNotNull( completedIndexConfig.get( key( crs.getName(), MIN ) ) );
            assertNotNull( completedIndexConfig.get( key( crs.getName(), MAX ) ) );
        }
    }

    @Test
    void completeConfigurationMustNotOverrideExistingSettings()
    {
        // Given
        GenericNativeIndexProvider provider = new GenericNativeIndexProvider( IndexDirectoryStructure.NONE, null, null, null, null, false, Config.defaults() );
        Map<String,Value> existingSettings = new HashMap<>();
        CoordinateReferenceSystem existingCrs = CoordinateReferenceSystem.Cartesian;
        IntValue tableId = Values.intValue( existingCrs.getTable().getTableId() );
        IntValue code = Values.intValue( existingCrs.getCode() );
        IntValue dimension = Values.intValue( existingCrs.getDimension() );
        IntValue maxLevels = Values.intValue( 0 );
        DoubleArray min = Values.doubleArray( new double[]{0, 0} );
        DoubleArray max = Values.doubleArray( new double[]{1, 1} );
        existingSettings.put( key( existingCrs.getName(), TABLE_ID ), tableId );
        existingSettings.put( key( existingCrs.getName(), CODE ), code );
        existingSettings.put( key( existingCrs.getName(), DIMENSIONS ), dimension );
        existingSettings.put( key( existingCrs.getName(), MAX_LEVELS ), maxLevels );
        existingSettings.put( key( existingCrs.getName(), MIN ), min );
        existingSettings.put( key( existingCrs.getName(), MAX ), max );
        IndexConfig existingIndexConfig = IndexConfig.with( existingSettings );
        LabelSchemaDescriptor incompleteSchema = SchemaDescriptor.forLabel( 1, 1 ).withIndexConfig( existingIndexConfig );
        IndexDescriptor incompleteDescriptor = IndexPrototype.forSchema( incompleteSchema, IndexProviderDescriptor.UNDECIDED )
                .withName( "index" ).materialise( 1 );

        // When
        IndexDescriptor completedDescriptor = provider.completeConfiguration( incompleteDescriptor );
        SchemaDescriptor completedSchema = completedDescriptor.schema();

        // Then
        IndexConfig completedIndexConfig = completedSchema.getIndexConfig();
        for ( CoordinateReferenceSystem crs : CoordinateReferenceSystem.all() )
        {
            if ( crs.equals( existingCrs ) )
            {
                // Assert value
                assertEquals( tableId, completedIndexConfig.get( key( crs.getName(), TABLE_ID ) ) );
                assertEquals( code, completedIndexConfig.get( key( crs.getName(), CODE ) ) );
                assertEquals( dimension, completedIndexConfig.get( key( crs.getName(), DIMENSIONS ) ) );
                assertEquals( maxLevels, completedIndexConfig.get( key( crs.getName(), MAX_LEVELS ) ) );
                assertEquals( min, completedIndexConfig.get( key( crs.getName(), MIN ) ) );
                assertEquals( max, completedIndexConfig.get( key( crs.getName(), MAX ) ) );
            }
            else
            {
                // Simply assert not null
                assertNotNull( completedIndexConfig.get( key( crs.getName(), TABLE_ID ) ) );
                assertNotNull( completedIndexConfig.get( key( crs.getName(), CODE ) ) );
                assertNotNull( completedIndexConfig.get( key( crs.getName(), DIMENSIONS ) ) );
                assertNotNull( completedIndexConfig.get( key( crs.getName(), MAX_LEVELS ) ) );
                assertNotNull( completedIndexConfig.get( key( crs.getName(), MIN ) ) );
                assertNotNull( completedIndexConfig.get( key( crs.getName(), MAX ) ) );
            }
        }
    }
}
