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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Header;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.index.schema.SpatialIndexConfig;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;

import static org.neo4j.io.fs.FileUtils.path;

/**
 * This class is the amber in which 3.5 generic index config reading is preserved in.
 * It has the ability to extract index configuration from a 3.5 generic index file, given
 * the root directory that provider in 3.5.
 */
class GenericConfigExtractor
{
    static IndexConfig indexConfigFromGenericFile( PageCache pageCache, File rootDir, long indexId ) throws IOException
    {
        File genericFile = path( rootDir, String.valueOf( indexId ), "index-" + indexId );
        Map<String,Value> indexConfig = new HashMap<>();
        GBPTree.readHeader( pageCache, genericFile, new GenericConfig( indexConfig ) );
        return IndexConfig.with( indexConfig );
    }

    // Copy of SpaceFillingCurveSettingsReader
    private static class GenericConfig implements Header.Reader
    {
        private static final byte VERSION = 0;
        private static final byte BYTE_FAILED = 0;
        private final Map<String,Value> indexConfig;
        //todo
        // - decide what to do if index configuration could not be read. Simply fallback to default bless?
        private boolean readSuccessful;

        GenericConfig( Map<String,Value> indexConfig )
        {
            this.indexConfig = indexConfig;
        }

        @Override
        public void read( ByteBuffer headerBytes )
        {
            byte state = headerBytes.get();
            if ( state == BYTE_FAILED )
            {
                readSuccessful = false;
            }
            else
            {
                byte version = headerBytes.get();
                if ( version != VERSION )
                {
                    throw new UnsupportedOperationException( "Invalid crs settings header version " + version + ", was expecting " + VERSION );
                }

                int count = headerBytes.getInt();
                for ( int i = 0; i < count; i++ )
                {
                    readNext( headerBytes );
                }
            }
        }

        private void readNext( ByteBuffer headerBytes )
        {
            int tableId = headerBytes.get() & 0xFF;
            int code = headerBytes.getInt();
            int maxLevels = headerBytes.getShort() & 0xFFFF;
            int dimensions = headerBytes.getShort() & 0xFFFF;
            double[] min = new double[dimensions];
            double[] max = new double[dimensions];
            for ( int i = 0; i < dimensions; i++ )
            {
                min[i] = Double.longBitsToDouble( headerBytes.getLong() );
                max[i] = Double.longBitsToDouble( headerBytes.getLong() );
            }
            CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( tableId, code );
            SpatialIndexConfig.addSpatialConfig( indexConfig, crs, dimensions, maxLevels, min, max );
        }
    }
}
