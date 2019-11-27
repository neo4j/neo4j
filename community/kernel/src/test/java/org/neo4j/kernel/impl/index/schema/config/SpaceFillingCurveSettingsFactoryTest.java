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

import java.util.Arrays;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.assertj.core.api.Assertions.assertThat;

class SpaceFillingCurveSettingsFactoryTest
{
    @Test
    void shouldGetDefaultSpaceFillingCurveSettingsForWGS84()
    {
        shouldGetSettingsFor( Config.defaults(), CoordinateReferenceSystem.WGS84, 2, new Envelope( -180, 180, -90, 90 ) );
    }

    @Test
    void shouldGetDefaultSpaceFillingCurveSettingsForWGS84_3D()
    {
        shouldGetSettingsFor( Config.defaults(), CoordinateReferenceSystem.WGS84_3D, 3,
                new Envelope( new double[]{-180, -90, -1000000}, new double[]{180, 90, 1000000} ) );
    }

    @Test
    void shouldGetDefaultSpaceFillingCurveSettingsForCartesian()
    {
        shouldGetSettingsFor( Config.defaults(), CoordinateReferenceSystem.Cartesian, 2, new Envelope( -1000000, 1000000, -1000000, 1000000 ) );
    }

    @Test
    void shouldGetDefaultSpaceFillingCurveSettingsForCartesian_3D()
    {
        shouldGetSettingsFor( Config.defaults(), CoordinateReferenceSystem.Cartesian_3D, 3,
                new Envelope( new double[]{-1000000, -1000000, -1000000}, new double[]{1000000, 1000000, 1000000} ) );
    }

    @Test
    void shouldGetModifiedSpaceFillingCurveSettingsForWGS84()
    {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS84;
        for ( int minx = -180; minx < 0; minx += 45 )
        {
            for ( int miny = -180; miny < 0; miny += 45 )
            {
                for ( int width = 10; width < 90; width += 40 )
                {
                    for ( int height = 10; height < 90; height += 40 )
                    {
                        shouldGetCustomSettingsFor( crs, new double[]{minx, miny}, new double[]{minx + width, miny + height} );
                    }
                }
            }
        }
    }

    @Test
    void shouldGetModifiedSpaceFillingCurveSettingsForWGS84_3D()
    {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS84_3D;
        shouldGetCustomSettingsFor( crs, new double[]{-180, -90, -1000000}, new double[]{180, 90, 1000000} );
        shouldGetCustomSettingsFor( crs, new double[]{0, -90, -1000000}, new double[]{180, 0, 1000000} );
        shouldGetCustomSettingsFor( crs, new double[]{-90, -45, -1000}, new double[]{90, 45, 1000} );
        shouldGetCustomSettingsFor( crs, new double[]{-90, -90, -1000}, new double[]{90, 45, 1000} );
        // invalid geographic limits should not affect settings or even the index, but will affect distance and bbox calculations
        shouldGetCustomSettingsFor( crs, new double[]{-1000, -1000, -1000}, new double[]{1000, 1000, 1000} );
    }

    @Test
    void shouldGetModifiedSpaceFillingCurveSettingsForCartesian()
    {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.Cartesian;
        for ( int maxBits = 30; maxBits <= 60; maxBits += 10 )
        {
            for ( int minx = -1000000; minx < 0; minx += 200000 )
            {
                for ( int miny = -1000000; miny < 0; miny += 2000000 )
                {
                    for ( int width = 100000; width < 1000000; width += 200000 )
                    {
                        for ( int height = 100000; height < 1000000; height += 200000 )
                        {
                            shouldGetCustomSettingsFor( crs, new double[]{minx, miny}, new double[]{minx + width, miny + height} );
                        }
                    }
                }
            }
        }
    }

    @Test
    void shouldGetModifiedSpaceFillingCurveSettingsForCartesian_3D()
    {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.Cartesian_3D;
        shouldGetCustomSettingsFor( crs, new double[]{-1000000, -1000000, -1000000}, new double[]{1000000, 1000000, 1000000} );
        shouldGetCustomSettingsFor( crs, new double[]{0, -1000000, -1000000}, new double[]{1000000, 0, 1000000} );
        shouldGetCustomSettingsFor( crs, new double[]{-1000, -1000, -1000}, new double[]{1000, 1000, 1000} );
        shouldGetCustomSettingsFor( crs, new double[]{-1000000000, -1000000000, -1000000000}, new double[]{1000000000, 1000000000, 1000000000} );
    }

    private void shouldGetCustomSettingsFor( CoordinateReferenceSystem crs, double[] min, double[] max )
    {
        CrsConfig crsConf = CrsConfig.group( crs );
        Config config = Config.newBuilder()
                .set( crsConf.min, Arrays.stream( min ).boxed().collect( Collectors.toList() ) )
                .set( crsConf.max, Arrays.stream( max ).boxed().collect( Collectors.toList() ) )
                .build();
        shouldGetSettingsFor( config, crs, min.length, new Envelope( min, max ) );
    }

    private void shouldGetSettingsFor( Config config, CoordinateReferenceSystem crs, int dimensions, Envelope envelope )
    {
        ConfiguredSpaceFillingCurveSettingsCache configuredSettings = new ConfiguredSpaceFillingCurveSettingsCache( config );
        SpaceFillingCurveSettings settings = configuredSettings.forCRS( crs );
        assertThat( settings.getDimensions() ).as( "Expected " + dimensions + "D for " + crs.getName() ).isEqualTo( dimensions );
        assertThat( settings.indexExtents() ).as( "Should have normal geographic 2D extents" ).isEqualTo( envelope );
    }
}
