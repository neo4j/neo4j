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

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Header;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;

import static org.neo4j.io.fs.FileUtils.path;

/**
 * This class is the amber in which 3.5 spatial index provider is preserved in.
 * Specifically it makes it possible to extract index configuration from an existing directory that contains
 * the old spatial index provider directory structure with one index file for each coordinate reference system.
 */
class SpatialConfigExtractor
{
    private static final Pattern CRS_FILE_PATTERN = Pattern.compile( "(\\d+)-(\\d+)" );
    private static final String spatialDirectoryName = "spatial-1.0";

    static Map<String,Value> indexConfigFromSpatialFile( FileSystemAbstraction fs, PageCache pageCache, File parentDir, long indexId )
            throws IOException
    {
        File spatialDirectory = path( parentDir, String.valueOf( indexId ), spatialDirectoryName );
        Map<String,Value> map = new HashMap<>();
        List<SpatialFile> spatialFiles = existingSpatialFiles( fs, spatialDirectory );
        for ( SpatialFile spatialFile : spatialFiles )
        {
            System.out.println( "  spatial file: " + spatialFile.indexFile );
            SpacialConfig spacialConfig = new SpacialConfig();
            GBPTree.readHeader( pageCache, spatialFile.indexFile, spacialConfig.headerReader() );

            CoordinateReferenceSystem crs = spatialFile.crs;
            int dimensions = spacialConfig.dimensions;
            int maxLevels = spacialConfig.maxLevels;
            double[] min = spacialConfig.min;
            double[] max = spacialConfig.max;
            addSpatialConfigToMap( map, crs, dimensions, maxLevels, min, max );
        }
        return map;
    }

    private static void addSpatialConfigToMap( Map<String,Value> map, CoordinateReferenceSystem crs, int dimensions, int maxLevels, double[] min,
            double[] max )
    {
        //todo Implement this
        // - Real target index provider needs to be called here so that we use correct name for all config options.
        // - Real target index provider also need to bless the config before returning.
        System.out.println( "  crs = " + crs );
        System.out.println( "    dimensions = " + dimensions );
        System.out.println( "    max = " + maxLevels );
        System.out.println( "    min = " + Arrays.toString( min ) );
        System.out.println( "    max = " + Arrays.toString( max ) );
    }

    private static List<SpatialFile> existingSpatialFiles( FileSystemAbstraction fs, File spatialDirectory )
    {
        List<SpatialFile> spatialFiles = new ArrayList<>();
        File[] files = fs.listFiles( spatialDirectory );
        if ( files != null )
        {
            for ( File file : files )
            {
                String name = file.getName();
                Matcher matcher = CRS_FILE_PATTERN.matcher( name );
                if ( matcher.matches() )
                {
                    int tableId = Integer.parseInt( matcher.group( 1 ) );
                    int code = Integer.parseInt( matcher.group( 2 ) );
                    CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( tableId, code );
                    spatialFiles.add( new SpatialFile( crs, file ) );
                }
            }
        }
        return spatialFiles;
    }

    private static class SpacialConfig
    {
        private static final byte BYTE_FAILED = 0;
        private static final int SPATIAL_INDEX_TYPE_SPACE_FILLING_CURVE = 1;
        private int dimensions;
        private int maxLevels;
        private double[] min;
        private double[] max;

        //todo
        // - decide what to do if index configuration could not be read. Simply fallback to default bless?
        private boolean readSuccessful;

        Header.Reader headerReader()
        {
            return headerBytes ->
            {
                byte state = headerBytes.get();
                if ( state == BYTE_FAILED )
                {
                    readSuccessful = false;
                }
                else
                {
                    int typeId = headerBytes.getInt();
                    if ( typeId == SPATIAL_INDEX_TYPE_SPACE_FILLING_CURVE )
                    {
                        try
                        {
                            maxLevels = headerBytes.getInt();
                            dimensions = headerBytes.getInt();
                            min = new double[dimensions];
                            max = new double[dimensions];
                            for ( int i = 0; i < dimensions; i++ )
                            {
                                min[i] = headerBytes.getDouble();
                                max[i] = headerBytes.getDouble();
                            }
                            readSuccessful = true;
                        }
                        catch ( BufferUnderflowException e )
                        {
                            readSuccessful = false;
                        }
                    }
                    else
                    {
                        readSuccessful = false;
                    }
                }
            };
        }
    }

    private static class SpatialFile
    {
        private final File indexFile;
        private final CoordinateReferenceSystem crs;

        SpatialFile( CoordinateReferenceSystem crs, File indexFile )
        {
            this.crs = crs;
            this.indexFile = indexFile;
        }
    }
}
