/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsFactory;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.IMMEDIATE;

public class SpatialUniqueSchemaIndexAccessorTest extends NativeSchemaIndexAccessorTest<SpatialSchemaKey,NativeSchemaValue>
{
    private static final CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS84;
    private static final SpaceFillingCurveSettings settings = new SpaceFillingCurveSettingsFactory( Config.defaults() ).settingsFor( crs );

    SpatialIndexFiles.SpatialFileLayout fileLayout;

    @Override
    NativeSchemaIndexAccessor<SpatialSchemaKey,NativeSchemaValue> makeAccessorWithSamplingConfig( IndexSamplingConfig samplingConfig ) throws IOException
    {
        fileLayout = new SpatialIndexFiles.SpatialFileLayout( CoordinateReferenceSystem.WGS84, settings, super.getIndexFile() );
        SpatialIndexFiles.SpatialFileLayout fileLayout =
                new SpatialIndexFiles.SpatialFileLayout( CoordinateReferenceSystem.WGS84, settings, super.getIndexFile() );
        return new SpatialIndexAccessor.PartAccessor( pageCache, fs, fileLayout, IMMEDIATE, monitor, schemaIndexDescriptor, indexId, samplingConfig,
                new StandardConfiguration() );
    }

    @Override
    public File getIndexFile()
    {
        return fileLayout.indexFile;
    }

    @Override
    protected LayoutTestUtil<SpatialSchemaKey,NativeSchemaValue> createLayoutTestUtil()
    {
        return new UniqueLayoutTestUtil<>( new SpatialLayoutTestUtil( SchemaIndexDescriptorFactory.uniqueForLabel( 42, 666 ), settings, crs ) );
    }
}
