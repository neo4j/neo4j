/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.values.storable.CoordinateReferenceSystem;

public class SpatialUniqueIndexPopulatorTest extends NativeUniqueIndexPopulatorTest<SpatialIndexKey,NativeIndexValue>
{
    private static final CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS84;
    private static final ConfiguredSpaceFillingCurveSettingsCache configuredSettings = new ConfiguredSpaceFillingCurveSettingsCache( Config.defaults() );

    private SpatialIndexFiles.SpatialFile spatialFile;

    @Override
    NativeIndexPopulator<SpatialIndexKey,NativeIndexValue> createPopulator( IndexSamplingConfig samplingConfig )
    {
        spatialFile = new SpatialIndexFiles.SpatialFile( crs, configuredSettings, super.getIndexFile() );
        return new SpatialIndexPopulator.PartPopulator( pageCache, fs, spatialFile.getLayoutForNewIndex(), monitor, indexDescriptor, samplingConfig,
                new StandardConfiguration() );
    }

    @Override
    public File getIndexFile()
    {
        return spatialFile.indexFile;
    }

    @Override
    protected LayoutTestUtil<SpatialIndexKey,NativeIndexValue> createLayoutTestUtil()
    {
        return new UniqueLayoutTestUtil<>(
                new SpatialLayoutTestUtil( TestIndexDescriptorFactory.uniqueForLabel( 42, 666 ), configuredSettings.forCRS( crs ), crs ) );
    }
}
