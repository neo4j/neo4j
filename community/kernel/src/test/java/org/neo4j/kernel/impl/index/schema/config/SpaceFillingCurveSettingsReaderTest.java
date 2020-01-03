/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.junit.Assert.assertEquals;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84_3D;

class SpaceFillingCurveSettingsReaderTest
{
    @Test
    void shouldReadMultipleSettings()
    {
        // given
        ConfiguredSpaceFillingCurveSettingsCache globalSettings = new ConfiguredSpaceFillingCurveSettingsCache( Config.defaults() );
        Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> expectedSpecificSettings = new HashMap<>();
        rememberSettings( globalSettings, expectedSpecificSettings, WGS84, WGS84_3D, Cartesian );
        IndexSpecificSpaceFillingCurveSettingsCache specificSettings =
                new IndexSpecificSpaceFillingCurveSettingsCache( globalSettings, expectedSpecificSettings );
        SpaceFillingCurveSettingsWriter writer = new SpaceFillingCurveSettingsWriter( specificSettings );
        byte[] bytes = new byte[PageCache.PAGE_SIZE];
        writer.accept( new ByteArrayPageCursor( bytes ) );

        Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> readExpectedSettings = new HashMap<>();
        SpaceFillingCurveSettingsReader reader = new SpaceFillingCurveSettingsReader( readExpectedSettings );

        // when
        reader.read( ByteBuffer.wrap( bytes ) );

        // then
        assertEquals( expectedSpecificSettings, readExpectedSettings );
    }

    private void rememberSettings( ConfiguredSpaceFillingCurveSettingsCache globalSettings,
            Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> expectedSpecificSettings, CoordinateReferenceSystem... crss )
    {
        for ( CoordinateReferenceSystem crs : crss )
        {
            expectedSpecificSettings.put( crs, globalSettings.forCRS( crs ) );
        }
    }
}
