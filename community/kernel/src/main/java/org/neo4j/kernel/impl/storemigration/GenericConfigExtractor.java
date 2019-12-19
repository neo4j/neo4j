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
import org.neo4j.index.internal.gbptree.MetadataMismatchException;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.index.schema.SpatialIndexConfig;
import org.neo4j.logging.Log;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;

import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;
import static org.neo4j.kernel.impl.storemigration.IndexConfigExtractorUtil.logExtractionFailure;

/**
 * This class is the amber in which 3.5 generic index config reading is preserved in.
 * It has the ability to extract index configuration from a 3.5 generic index file, given
 * the root directory that provider in 3.5.
 */
final class GenericConfigExtractor
{
    private GenericConfigExtractor()
    {}

    static IndexConfig indexConfigFromGenericFile( FileSystemAbstraction fs, PageCache pageCache, File genericFile, Log log ) throws IOException
    {
        Map<String,Value> indexConfig = new HashMap<>();
        if ( fs.fileExists( genericFile ) )
        {
            try
            {
                GBPTree.readHeader( pageCache, genericFile, new GenericConfig( indexConfig, genericFile, log ), TRACER_SUPPLIER.get() );
            }
            catch ( MetadataMismatchException e )
            {
                logExtractionFailure( "Index meta data is corrupt and can not be parsed.", log, genericFile );
            }
        }
        else
        {
            logExtractionFailure( "Index file does not exists.", log, genericFile );
        }
        return IndexConfig.with( indexConfig );
    }

    // Copy of SpaceFillingCurveSettingsReader
    private static class GenericConfig implements Header.Reader
    {
        private static final byte VERSION = 0;
        private static final byte BYTE_FAILED = 0;
        private final Map<String,Value> indexConfig;
        private final File indexFile;
        private final Log log;

        GenericConfig( Map<String,Value> indexConfig, File indexFile, Log log )
        {
            this.indexConfig = indexConfig;
            this.indexFile = indexFile;
            this.log = log;
        }

        @Override
        public void read( ByteBuffer headerBytes )
        {
            byte state = headerBytes.get();
            if ( state != BYTE_FAILED )
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
            else
            {
                // We can't extract index configuration from broken index.
                // Warn about this and let index provider add default settings to index later on in migration.
                logExtractionFailure( "Index is in FAILED state.", log, indexFile );
            }
        }

        private void readNext( ByteBuffer headerBytes )
        {
            int tableId = headerBytes.get() & 0xFF;
            int code = headerBytes.getInt();
            //noinspection unused
            int maxLevels = headerBytes.getShort() & 0xFFFF; // Will not be migrated but read to progress cursor
            int dimensions = headerBytes.getShort() & 0xFFFF;
            double[] min = new double[dimensions];
            double[] max = new double[dimensions];
            for ( int i = 0; i < dimensions; i++ )
            {
                min[i] = Double.longBitsToDouble( headerBytes.getLong() );
                max[i] = Double.longBitsToDouble( headerBytes.getLong() );
            }
            CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( tableId, code );
            SpatialIndexConfig.addSpatialConfig( indexConfig, crs, min, max );
        }
    }
}
