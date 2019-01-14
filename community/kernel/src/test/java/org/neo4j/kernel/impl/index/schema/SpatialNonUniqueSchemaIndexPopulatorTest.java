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

import java.io.File;

import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsFactory;
import org.neo4j.values.storable.CoordinateReferenceSystem;

public class SpatialNonUniqueSchemaIndexPopulatorTest extends NativeNonUniqueSchemaIndexPopulatorTest<SpatialSchemaKey,NativeSchemaValue>
{
    private static final CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS84;
    private static final SpaceFillingCurveSettingsFactory settings = new SpaceFillingCurveSettingsFactory( Config.defaults() );

    private SpatialIndexFiles.SpatialFile spatialFile;

    @Override
    NativeSchemaIndexPopulator<SpatialSchemaKey,NativeSchemaValue> createPopulator( IndexSamplingConfig samplingConfig )
    {
        spatialFile = new SpatialIndexFiles.SpatialFile( crs, settings, super.getIndexFile() );
        return new SpatialIndexPopulator.PartPopulator( pageCache, fs, spatialFile.getLayoutForNewIndex(), monitor, schemaIndexDescriptor, indexId,
                samplingConfig, new StandardConfiguration() );
    }

    @Override
    public File getIndexFile()
    {
        return spatialFile.indexFile;
    }

    @Override
    protected LayoutTestUtil<SpatialSchemaKey,NativeSchemaValue> createLayoutTestUtil()
    {
        return new SpatialLayoutTestUtil( SchemaIndexDescriptorFactory.forLabel( 42, 666 ), settings.settingsFor( crs ), crs );
    }
}
