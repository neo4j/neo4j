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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsFactory;
import org.neo4j.values.storable.CoordinateReferenceSystem;

class SpatialIndexFiles
{
    private static final Pattern CRS_DIR_PATTERN = Pattern.compile( "(\\d+)-(\\d+)" );
    private final FileSystemAbstraction fs;
    private final SpaceFillingCurveSettingsFactory settingsFactory;
    private final File indexDirectory;

    SpatialIndexFiles( IndexDirectoryStructure directoryStructure, long indexId, FileSystemAbstraction fs, SpaceFillingCurveSettingsFactory settingsFactory )
    {
        this.fs = fs;
        this.settingsFactory = settingsFactory;
        indexDirectory = directoryStructure.directoryForIndex( indexId );
    }

    Iterable<SpatialFile> existing()
    {
        List<SpatialFile> existing = new ArrayList<>();
        addExistingFiles( existing );
        return existing;
    }

    <T> void loadExistingIndexes( SpatialIndexCache<T> indexCache ) throws IOException
    {
        for ( SpatialFile fileLayout : existing() )
        {
            indexCache.select( fileLayout.crs );
        }
    }

    SpatialFile forCrs( CoordinateReferenceSystem crs )
    {
        return new SpatialFile( crs, settingsFactory, indexDirectory );
    }

    private void addExistingFiles( List<SpatialFile> existing )
    {
        File[] files = fs.listFiles( indexDirectory );
        if ( files != null )
        {
            for ( File file : files )
            {
                String name = file.getName();
                Matcher matcher = CRS_DIR_PATTERN.matcher( name );
                if ( matcher.matches() )
                {
                    int tableId = Integer.parseInt( matcher.group( 1 ) );
                    int code = Integer.parseInt( matcher.group( 2 ) );
                    CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( tableId, code );
                    existing.add( forCrs( crs ) );
                }
            }
        }
    }

    static class SpatialFile
    {
        final File indexFile;
        final SpaceFillingCurveSettingsFactory settings;
        private final CoordinateReferenceSystem crs;

        SpatialFile( CoordinateReferenceSystem crs, SpaceFillingCurveSettingsFactory settingsFactory, File indexDirectory )
        {
            this.crs = crs;
            this.settings = settingsFactory;
            String s = crs.getTable().getTableId() + "-" + Integer.toString( crs.getCode() );
            this.indexFile = new File( indexDirectory, s );
        }

        /**
         * If this is the first time an index is being created, get the layout settings from the config settings only
         */
        SpatialFileLayout getLayoutForNewIndex()
        {
            return new SpatialFileLayout( this, settings.settingsFor( crs ) );
        }

        /**
         * If we are loading a layout for an existing index, read the settings from the index header, and ignore config settings
         */
        SpatialFileLayout getLayoutForExistingIndex( PageCache pageCache ) throws IOException
        {
            SpaceFillingCurveSettings settings =
                    SpaceFillingCurveSettings.fromGBPTree( indexFile, pageCache, NativeSchemaIndexHeaderReader::readFailureMessage );
            return new SpatialFileLayout( this, settings );
        }
    }

    static class SpatialFileLayout
    {
        final SpaceFillingCurveSettings settings;
        final SpatialFile spatialFile;
        final Layout<SpatialSchemaKey,NativeSchemaValue> layout;

        SpatialFileLayout( SpatialFile spatialFile, SpaceFillingCurveSettings settings )
        {
            this.spatialFile = spatialFile;
            this.settings = settings;
            this.layout = new SpatialLayout( spatialFile.crs, settings.curve() );
        }

        public File getIndexFile()
        {
            return spatialFile.indexFile;
        }
    }
}
