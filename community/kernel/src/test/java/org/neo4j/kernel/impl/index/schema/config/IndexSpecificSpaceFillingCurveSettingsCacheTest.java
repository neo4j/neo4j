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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;

class IndexSpecificSpaceFillingCurveSettingsCacheTest
{
    private final ConfiguredSpaceFillingCurveSettingsCache globalSettings = new ConfiguredSpaceFillingCurveSettingsCache( Config.defaults() );

    @Test
    void shouldHaveInitialIndexSpecificSetting()
    {
        // given
        Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> initialSettings = new HashMap<>();
        initialSettings.put( WGS84, globalSettings.forCRS( WGS84 ) );
        initialSettings.put( Cartesian, globalSettings.forCRS( Cartesian ) );
        IndexSpecificSpaceFillingCurveSettingsCache indexSettings = new IndexSpecificSpaceFillingCurveSettingsCache( globalSettings, initialSettings );

        // when
        ToMapSettingVisitor visitor = new ToMapSettingVisitor();
        indexSettings.visitIndexSpecificSettings( visitor );

        // then
        assertEquals( initialSettings, visitor.map );
    }

    @Test
    void shouldHaveInitialIndexSpecificSettingsPlusRequestedOnes()
    {
        // given
        Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> initialSettings = new HashMap<>();
        initialSettings.put( WGS84, globalSettings.forCRS( WGS84 ) );
        initialSettings.put( Cartesian, globalSettings.forCRS( Cartesian ) );
        IndexSpecificSpaceFillingCurveSettingsCache indexSettings = new IndexSpecificSpaceFillingCurveSettingsCache( globalSettings, initialSettings );

        // when
        indexSettings.forCrs( Cartesian_3D, true );

        // then
        ToMapSettingVisitor visitor = new ToMapSettingVisitor();
        indexSettings.visitIndexSpecificSettings( visitor );
        Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> expectedSettings = new HashMap<>( initialSettings );
        assertNull( expectedSettings.put( Cartesian_3D, globalSettings.forCRS( Cartesian_3D ) ) );
        assertEquals( expectedSettings, visitor.map );
    }

    @Test
    void shouldNotCreateIndexSpecificSettingForReadRequest()
    {
        // given
        Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> initialSettings = new HashMap<>();
        initialSettings.put( WGS84, globalSettings.forCRS( WGS84 ) );
        initialSettings.put( Cartesian, globalSettings.forCRS( Cartesian ) );
        IndexSpecificSpaceFillingCurveSettingsCache indexSettings = new IndexSpecificSpaceFillingCurveSettingsCache( globalSettings, initialSettings );

        // when
        indexSettings.forCrs( Cartesian_3D, false );

        // then
        ToMapSettingVisitor visitor = new ToMapSettingVisitor();
        indexSettings.visitIndexSpecificSettings( visitor );
        assertEquals( initialSettings, visitor.map );
    }
}
