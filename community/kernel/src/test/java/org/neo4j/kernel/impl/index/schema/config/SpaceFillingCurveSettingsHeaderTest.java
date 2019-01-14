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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;

class SpaceFillingCurveSettingsHeaderTest
{
    private final byte[] data = new byte[PAGE_SIZE];
    private final PageCursor pageCursor = ByteArrayPageCursor.wrap( data );

    @Test
    void shouldWriteAndReadZeroSettings()
    {
        shouldWriteAndReadSettings();
    }

    @Test
    void shouldWriteAndReadSingleSetting()
    {
        shouldWriteAndReadSettings( CoordinateReferenceSystem.WGS84 );
    }

    @Test
    void shouldWriteAndReadMultipleSettings()
    {
        shouldWriteAndReadSettings( CoordinateReferenceSystem.WGS84, CoordinateReferenceSystem.Cartesian, CoordinateReferenceSystem.Cartesian_3D );
    }

    private void shouldWriteAndReadSettings( CoordinateReferenceSystem... crss )
    {
        // given
        IndexSpecificSpaceFillingCurveSettingsCache indexSettings =
                new IndexSpecificSpaceFillingCurveSettingsCache( new ConfiguredSpaceFillingCurveSettingsCache( Config.defaults() ), new HashMap<>() );
        for ( CoordinateReferenceSystem crs : crss )
        {
            indexSettings.forCrs( crs, true );
        }
        SpaceFillingCurveSettingsWriter writer = new SpaceFillingCurveSettingsWriter( indexSettings );

        // when
        writer.accept( pageCursor );
        pageCursor.rewind();

        // then
        Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> read = new HashMap<>();
        SpaceFillingCurveSettingsReader reader = new SpaceFillingCurveSettingsReader( read );
        reader.read( ByteBuffer.wrap( data ) );
        assertEquals( asMap( indexSettings ), read );
    }

    private Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> asMap( IndexSpecificSpaceFillingCurveSettingsCache indexSettings )
    {
        ToMapSettingVisitor visitor = new ToMapSettingVisitor();
        indexSettings.visitIndexSpecificSettings( visitor );
        return visitor.map;
    }
}
