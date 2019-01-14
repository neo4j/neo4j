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
package org.neo4j.kernel.impl.index.schema.config;

import java.nio.ByteBuffer;
import java.util.Map;

import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Header;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsWriter.VERSION;

/**
 * {@link GBPTree} header reader for reading {@link SpaceFillingCurveSettings}.
 *
 * @see SpaceFillingCurveSettingsWriter
 */
public class SpaceFillingCurveSettingsReader implements Header.Reader
{
    private final Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> settings;

    public SpaceFillingCurveSettingsReader( Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> settings )
    {
        this.settings = settings;
    }

    @Override
    public void read( ByteBuffer headerBytes )
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

    private void readNext( ByteBuffer headerBytes )
    {
        int tableId = headerBytes.get() & 0xFF;
        int code = headerBytes.getInt();
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( tableId, code );

        int maxLevels = headerBytes.getShort() & 0xFFFF;
        int dimensions = headerBytes.getShort() & 0xFFFF;
        double[] min = new double[dimensions];
        double[] max = new double[dimensions];
        for ( int i = 0; i < dimensions; i++ )
        {
            min[i] = Double.longBitsToDouble( headerBytes.getLong() );
            max[i] = Double.longBitsToDouble( headerBytes.getLong() );
        }
        Envelope extents = new Envelope( min, max );
        settings.put( crs, new SpaceFillingCurveSettings( dimensions, extents, maxLevels ) );
    }
}
