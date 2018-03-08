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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve2D;
import org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve3D;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.values.storable.CoordinateReferenceSystem;

class SpatialIndexFiles
{
    private static final Pattern CRS_DIR_PATTERN = Pattern.compile( "(\\d+)-(\\d+)" );
    private final FileSystemAbstraction fs;
    private final int maxBits;
    private final File indexDirectory;

    SpatialIndexFiles( IndexDirectoryStructure directoryStructure, long indexId, FileSystemAbstraction fs, int maxBits )
    {
        this.fs = fs;
        this.maxBits = maxBits;
        indexDirectory = directoryStructure.directoryForIndex( indexId );
    }

    Iterable<SpatialFileLayout> existing()
    {
        List<SpatialFileLayout> existing = new ArrayList<>();
        addExistingFiles( existing );
        return existing;
    }

    <T, E extends Exception> void loadExistingIndexes( SpatialIndexCache<T,E> indexCache ) throws E
    {
        for ( SpatialFileLayout fileLayout : existing() )
        {
            indexCache.select( fileLayout.crs );
        }
    }

    SpatialFileLayout forCrs( CoordinateReferenceSystem crs )
    {
        SpaceFillingCurve curve;
        if ( crs.getDimension() == 2 )
        {
            curve = new HilbertSpaceFillingCurve2D( envelopeFromCRS( crs ), Math.min( 30, maxBits / 2 ) );
        }
        else if ( crs.getDimension() == 3 )
        {
            curve = new HilbertSpaceFillingCurve3D( envelopeFromCRS( crs ), Math.min( 20, maxBits / 3 ) );
        }
        else
        {
            throw new IllegalArgumentException( "Cannot create spatial index with other than 2D or 3D coordinate reference system: " + crs );
        }
        String s = crs.getTable().getTableId() + "-" + Integer.toString( crs.getCode() );
        File file = new File( indexDirectory, s );
        return new SpatialFileLayout( file, new SpatialLayout( crs, curve ), crs );
    }

    private void addExistingFiles( List<SpatialFileLayout> existing )
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

    static class SpatialFileLayout
    {
        final File indexFile;
        final Layout<SpatialSchemaKey,NativeSchemaValue> layout;
        final CoordinateReferenceSystem crs;

        SpatialFileLayout( File indexFile, Layout<SpatialSchemaKey,NativeSchemaValue> layout, CoordinateReferenceSystem crs )
        {
            this.indexFile = indexFile;
            this.layout = layout;
            this.crs = crs;
        }
    }

    static Envelope envelopeFromCRS( CoordinateReferenceSystem crs )
    {
        Pair<double[],double[]> indexEnvelope = crs.getIndexEnvelope();
        return new Envelope( indexEnvelope.first(), indexEnvelope.other() );
    }
}
