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
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.graphdb.schema.IndexSettingUtil.spatialMaxSettingForCrs;
import static org.neo4j.graphdb.schema.IndexSettingUtil.spatialMinSettingForCrs;

class GenericNativeIndexProviderTest
{
    @Test
    void mustCompleteIndexDescriptorConfigurationsWithSpatialConfig()
    {
        // Given
        DatabaseIndexContext context = DatabaseIndexContext.builder( null, null ).build();
        GenericNativeIndexProvider provider = new GenericNativeIndexProvider( context, IndexDirectoryStructure.NONE, null, Config.defaults() );
        LabelSchemaDescriptor incompleteSchema = SchemaDescriptor.forLabel( 1, 1 );
        IndexDescriptor incompleteDescriptor = IndexPrototype.forSchema( incompleteSchema, IndexProviderDescriptor.UNDECIDED )
                .withName( "index" ).materialise( 1 );

        // When
        IndexDescriptor completedDescriptor = provider.completeConfiguration( incompleteDescriptor );

        // Then
        IndexConfig sinfulIndexConfig = incompleteDescriptor.getIndexConfig();
        IndexConfig completedIndexConfig = completedDescriptor.getIndexConfig();
        assertEquals( 0, sinfulIndexConfig.entries().count( p -> true ), "expected sinful index config to have no entries" );
        for ( CoordinateReferenceSystem crs : CoordinateReferenceSystem.all() )
        {
            assertNotNull( completedIndexConfig.get( spatialMinSettingForCrs( crs ).getSettingName() ) );
            assertNotNull( completedIndexConfig.get( spatialMaxSettingForCrs( crs ).getSettingName() ) );
        }
    }

    @Test
    void completeConfigurationMustNotOverrideExistingSettings()
    {
        // Given
        DatabaseIndexContext context = DatabaseIndexContext.builder( null, null ).build();
        GenericNativeIndexProvider provider = new GenericNativeIndexProvider( context, IndexDirectoryStructure.NONE, null, Config.defaults() );
        Map<String,Value> existingSettings = new HashMap<>();
        CoordinateReferenceSystem existingCrs = CoordinateReferenceSystem.Cartesian;
        DoubleArray min = Values.doubleArray( new double[]{0, 0} );
        DoubleArray max = Values.doubleArray( new double[]{1, 1} );
        existingSettings.put( spatialMinSettingForCrs( existingCrs ).getSettingName(), min );
        existingSettings.put( spatialMaxSettingForCrs( existingCrs ).getSettingName(), max );
        IndexConfig existingIndexConfig = IndexConfig.with( existingSettings );
        LabelSchemaDescriptor incompleteSchema = SchemaDescriptor.forLabel( 1, 1 );
        IndexDescriptor incompleteDescriptor = IndexPrototype.forSchema( incompleteSchema, IndexProviderDescriptor.UNDECIDED )
                .withName( "index" ).materialise( 1 ).withIndexConfig( existingIndexConfig );

        // When
        IndexDescriptor completedDescriptor = provider.completeConfiguration( incompleteDescriptor );

        // Then
        IndexConfig completedIndexConfig = completedDescriptor.getIndexConfig();
        for ( CoordinateReferenceSystem crs : CoordinateReferenceSystem.all() )
        {
            if ( crs.equals( existingCrs ) )
            {
                // Assert value
                assertEquals( min, completedIndexConfig.get( spatialMinSettingForCrs( crs ).getSettingName() ) );
                assertEquals( max, completedIndexConfig.get( spatialMaxSettingForCrs( crs ).getSettingName() ) );
            }
            else
            {
                // Simply assert not null
                assertNotNull( completedIndexConfig.get( spatialMinSettingForCrs( crs ).getSettingName() ) );
                assertNotNull( completedIndexConfig.get( spatialMaxSettingForCrs( crs ).getSettingName() ) );
            }
        }
    }
}
