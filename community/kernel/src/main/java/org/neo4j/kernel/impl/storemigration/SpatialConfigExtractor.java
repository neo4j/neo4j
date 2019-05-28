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
package org.neo4j.kernel.impl.storemigration;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Header;
import org.neo4j.index.internal.gbptree.MetadataMismatchException;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.index.schema.SpatialIndexConfig;
import org.neo4j.logging.Log;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.storemigration.IndexConfigExtractorUtil.logExtractionFailure;

/**
 * This class is the amber in which 3.5 spatial index provider is preserved in.
 * Specifically it makes it possible to extract index configuration from an existing directory that contains
 * the old spatial index provider directory structure with one index file for each coordinate reference system.
 */
final class SpatialConfigExtractor
{
    private static final byte BYTE_FAILED = 0;
    private static final int SPATIAL_INDEX_TYPE_SPACE_FILLING_CURVE = 1;

    private SpatialConfigExtractor()
    {
    }

    static IndexConfig indexConfigFromSpatialFile( PageCache pageCache, List<SpatialFile> spatialFiles, Log log ) throws IOException
    {
        Map<String,Value> map = new HashMap<>();
        for ( SpatialFile spatialFile : spatialFiles )
        {
            try
            {
                GBPTree.readHeader( pageCache, spatialFile.getIndexFile(), headerReader( map, spatialFile, log ) );
            }
            catch ( MetadataMismatchException e )
            {
                logExtractionFailure( "Index meta data is corrupt and can not be parsed.", log, spatialFile.getIndexFile() );
                map = Collections.emptyMap();
                break;
            }
        }
        return IndexConfig.with( map );
    }

    private static Header.Reader headerReader( Map<String,Value> map, SpatialFile spatialFile, Log log )
    {
        return headerBytes ->
        {
            byte state = headerBytes.get();
            if ( state != BYTE_FAILED )
            {
                int typeId = headerBytes.getInt();
                if ( typeId == SPATIAL_INDEX_TYPE_SPACE_FILLING_CURVE )
                {
                    try
                    {
                        int maxLevels = headerBytes.getInt();
                        int dimensions = headerBytes.getInt();
                        double[] min = new double[dimensions];
                        double[] max = new double[dimensions];
                        for ( int i = 0; i < dimensions; i++ )
                        {
                            min[i] = headerBytes.getDouble();
                            max[i] = headerBytes.getDouble();
                        }
                        CoordinateReferenceSystem crs = spatialFile.getCrs();
                        SpatialIndexConfig.addSpatialConfig( map, crs, dimensions, maxLevels, min, max );
                    }
                    catch ( BufferUnderflowException e )
                    {
                        logExtractionFailure( "Got an exception, " + e.toString() + ".", log, spatialFile.getIndexFile() );
                    }
                }
                else
                {
                    logExtractionFailure( "Spatial index file is of an unknown type, typeId=" + state + ".", log, spatialFile.getIndexFile() );
                }
            }
            else
            {
                logExtractionFailure( "Index is in FAILED state.", log, spatialFile.getIndexFile() );
            }
        };
    }
}
