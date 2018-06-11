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
import java.io.IOException;

import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsFactory;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.IMMEDIATE;

abstract class SpatialIndexAccessorTest extends NativeIndexAccessorTest<SpatialIndexKey,NativeIndexValue>
{
    static final CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS84;
    static final SpaceFillingCurveSettings settings = new SpaceFillingCurveSettingsFactory( Config.defaults() ).settingsFor( crs );

    SpatialIndexFiles.SpatialFileLayout fileLayout;

    @Override
    NativeIndexAccessor<SpatialIndexKey,NativeIndexValue> makeAccessorWithSamplingConfig( IndexSamplingConfig samplingConfig ) throws IOException
    {
        fileLayout = new SpatialIndexFiles.SpatialFileLayout( CoordinateReferenceSystem.WGS84, settings, super.getIndexFile() );
        SpatialIndexFiles.SpatialFileLayout fileLayout =
                new SpatialIndexFiles.SpatialFileLayout( CoordinateReferenceSystem.WGS84, settings, super.getIndexFile() );
        return new SpatialIndexAccessor.PartAccessor( pageCache, fs, fileLayout, IMMEDIATE, monitor, indexDescriptor, samplingConfig,
                new StandardConfiguration() );
    }

    @Override
    public File getIndexFile()
    {
        return fileLayout.indexFile;
    }

    @Override
    public void shouldNotSeeFilteredEntries()
    {
        // This test doesn't make sense for spatial, since it needs a proper store for the values
    }

    @Override
    public void shouldReturnMatchingEntriesForRangePredicateWithExclusiveStartAndExclusiveEnd()
    {
        // Exclusive is handled via a postfilter for spatial
    }

    @Override
    public void shouldReturnMatchingEntriesForRangePredicateWithExclusiveStartAndInclusiveEnd()
    {
        // Exclusive is handled via a postfilter for spatial
    }

    @Override
    public void shouldReturnMatchingEntriesForRangePredicateWithInclusiveStartAndExclusiveEnd()
    {
        // Exclusive is handled via a postfilter for spatial
    }
}
