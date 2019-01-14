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
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.values.storable.CoordinateReferenceSystem;

public class SpatialUniqueIndexPopulatorTest extends NativeIndexPopulatorTests.Unique<SpatialIndexKey,NativeIndexValue>
{
    private static final CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS84;
    private static final ConfiguredSpaceFillingCurveSettingsCache configuredSettings = new ConfiguredSpaceFillingCurveSettingsCache( Config.defaults() );

    private SpatialIndexFiles.SpatialFile spatialFile;

    @Override
    NativeIndexPopulator<SpatialIndexKey,NativeIndexValue> createPopulator()
    {
        spatialFile = new SpatialIndexFiles.SpatialFile( crs, configuredSettings, super.getIndexFile() );
        return new SpatialIndexPopulator.PartPopulator( pageCache, fs, spatialFile.getLayoutForNewIndex(), monitor, indexDescriptor,
                new StandardConfiguration() );
    }

    @Override
    public File getIndexFile()
    {
        return spatialFile.indexFile;
    }

    @Override
    protected ValueCreatorUtil<SpatialIndexKey,NativeIndexValue> createValueCreatorUtil()
    {
        return new SpatialValueCreatorUtil( TestIndexDescriptorFactory.uniqueForLabel( 42, 666 ).withId( 0 ), ValueCreatorUtil.FRACTION_DUPLICATE_UNIQUE );
    }

    @Override
    IndexLayout<SpatialIndexKey,NativeIndexValue> createLayout()
    {
        return new SpatialLayout( crs, configuredSettings.forCRS( crs ).curve() );
    }

    @Override
    public void addShouldThrowOnDuplicateValues()
    {   // Spatial can not throw on duplicate values during population because it
        // might throw for points that are in fact unique. Instead, uniqueness will
        // be verified by ConstraintIndexCreator when population is finished.
    }

    @Override
    public void updaterShouldThrowOnDuplicateValues()
    {   // Spatial can not throw on duplicate values during population because it
        // might throw for points that are in fact unique. Instead, uniqueness will
        // be verified by ConstraintIndexCreator when population is finished.
    }
}
