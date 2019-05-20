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

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.exceptions.schema.MisconfiguredIndexException;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GenericNativeIndexProviderTest
{
    @Test
    void mustBlessIndexDescriptorWithSpatialConfig() throws MisconfiguredIndexException
    {
        // Given
        GenericNativeIndexProvider provider = new GenericNativeIndexProvider( IndexDirectoryStructure.NONE, null, null, null, null, false, Config.defaults() );
        LabelSchemaDescriptor sinfulSchema = SchemaDescriptor.forLabel( 1, 1 );
        IndexDescriptor sinfulDescriptor = IndexDescriptorFactory.forSchema( sinfulSchema, IndexProviderDescriptor.UNDECIDED );

        // When
        IndexDescriptor blessesDescriptor = provider.bless( sinfulDescriptor );
        SchemaDescriptor blessedSchema = blessesDescriptor.schema();

        // Then
        IndexConfig sinfulIndexConfig = sinfulSchema.getIndexConfig();
        IndexConfig blessedIndexConfig = blessedSchema.getIndexConfig();
        assertEquals( 0, sinfulIndexConfig.entries().count( p -> true ), "expected sinful index config to have no entries" );
        for ( CoordinateReferenceSystem crs : CoordinateReferenceSystem.all() )
        {
            String prefix = SpatialIndexConfig.SPATIAL_CONFIG_PREFIX + "." + crs.getName();
            assertNotNull( blessedIndexConfig.get( prefix + ".tableId" ) );
            assertNotNull( blessedIndexConfig.get( prefix + ".code" ) );
            assertNotNull( blessedIndexConfig.get( prefix + ".dimensions" ) );
            assertNotNull( blessedIndexConfig.get( prefix + ".maxLevels" ) );
            assertNotNull( blessedIndexConfig.get( prefix + ".min" ) );
            assertNotNull( blessedIndexConfig.get( prefix + ".max" ) );
        }
    }
}
