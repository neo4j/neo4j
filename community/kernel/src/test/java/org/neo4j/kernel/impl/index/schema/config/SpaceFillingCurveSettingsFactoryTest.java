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

import org.junit.Test;

import java.util.HashMap;

import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.nullValue;

public class SpaceFillingCurveSettingsFactoryTest
{
    @Test
    public void shouldGetDefaultSpaceFillingCurveSettingsForWGS84()
    {
        shouldGetSettingsFor( Config.defaults(), CoordinateReferenceSystem.WGS84, 2, 60, new Envelope( -180, 180, -90, 90 ) );
    }

    @Test
    public void shouldGetDefaultSpaceFillingCurveSettingsForWGS84_3D()
    {
        shouldGetSettingsFor( Config.defaults(), CoordinateReferenceSystem.WGS84_3D, 3, 60,
                new Envelope( new double[]{-180, -90, -1000000}, new double[]{180, 90, 1000000} ) );
    }

    @Test
    public void shouldGetDefaultSpaceFillingCurveSettingsForCartesian()
    {
        shouldGetSettingsFor( Config.defaults(), CoordinateReferenceSystem.Cartesian, 2, 60, new Envelope( -1000000, 1000000, -1000000, 1000000 ) );
    }

    @Test
    public void shouldGetDefaultSpaceFillingCurveSettingsForCartesian_3D()
    {
        shouldGetSettingsFor( Config.defaults(), CoordinateReferenceSystem.Cartesian_3D, 3, 60,
                new Envelope( new double[]{-1000000, -1000000, -1000000}, new double[]{1000000, 1000000, 1000000} ) );
    }

    @Test
    public void shouldGetModifiedSpaceFillingCurveSettingsForWGS84()
    {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS84;
        for ( int maxBits = 30; maxBits <= 60; maxBits += 10 )
        {
            for ( int minx = -180; minx < 0; minx += 45 )
            {
                for ( int miny = -180; miny < 0; miny += 45 )
                {
                    for ( int width = 10; width < 90; width += 40 )
                    {
                        for ( int height = 10; height < 90; height += 40 )
                        {
                            shouldGetCustomSettingsFor( crs, maxBits, new double[]{minx, miny}, new double[]{minx + width, miny + height} );
                        }
                    }
                }
            }
        }
    }

    @Test
    public void shouldGetModifiedSpaceFillingCurveSettingsForWGS84_3D()
    {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS84_3D;
        shouldGetCustomSettingsFor( crs, 60, new double[]{-180, -90, -1000000}, new double[]{180, 90, 1000000} );
        shouldGetCustomSettingsFor( crs, 30, new double[]{-180, -90, -1000000}, new double[]{180, 90, 1000000} );
        shouldGetCustomSettingsFor( crs, 60, new double[]{0, -90, -1000000}, new double[]{180, 0, 1000000} );
        shouldGetCustomSettingsFor( crs, 30, new double[]{0, -90, -1000000}, new double[]{180, 0, 1000000} );
        shouldGetCustomSettingsFor( crs, 60, new double[]{-90, -45, -1000}, new double[]{90, 45, 1000} );
        shouldGetCustomSettingsFor( crs, 30, new double[]{-90, -90, -1000}, new double[]{90, 45, 1000} );
        // invalid geographic limits should not affect settings or even the index, but will affect distance and bbox calculations
        shouldGetCustomSettingsFor( crs, 60, new double[]{-1000, -1000, -1000}, new double[]{1000, 1000, 1000} );
        shouldGetCustomSettingsFor( crs, 30, new double[]{-1000, -1000, -1000}, new double[]{1000, 1000, 1000} );
    }

    @Test
    public void shouldGetModifiedSpaceFillingCurveSettingsForCartesian()
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
                            shouldGetCustomSettingsFor( crs, maxBits, new double[]{minx, miny}, new double[]{minx + width, miny + height} );
                        }
                    }
                }
            }
        }
    }

    @Test
    public void shouldGetModifiedSpaceFillingCurveSettingsForCartesian_3D()
    {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.Cartesian_3D;
        shouldGetCustomSettingsFor( crs, 60, new double[]{-1000000, -1000000, -1000000}, new double[]{1000000, 1000000, 1000000} );
        shouldGetCustomSettingsFor( crs, 30, new double[]{-1000000, -1000000, -1000000}, new double[]{1000000, 1000000, 1000000} );
        shouldGetCustomSettingsFor( crs, 60, new double[]{0, -1000000, -1000000}, new double[]{1000000, 0, 1000000} );
        shouldGetCustomSettingsFor( crs, 30, new double[]{0, -1000000, -1000000}, new double[]{1000000, 0, 1000000} );
        shouldGetCustomSettingsFor( crs, 60, new double[]{-1000, -1000, -1000}, new double[]{1000, 1000, 1000} );
        shouldGetCustomSettingsFor( crs, 30, new double[]{-1000, -1000, -1000}, new double[]{1000, 1000, 1000} );
        shouldGetCustomSettingsFor( crs, 60, new double[]{-1000000000, -1000000000, -1000000000}, new double[]{1000000000, 1000000000, 1000000000} );
        shouldGetCustomSettingsFor( crs, 30, new double[]{-1000000000, -1000000000, -1000000000}, new double[]{1000000000, 1000000000, 1000000000} );
    }

    private void shouldGetCustomSettingsFor( CoordinateReferenceSystem crs, int maxBits, double[] min, double[] max )
    {
        String crsPrefix = "unsupported.dbms.db.spatial.crs." + crs.getName();
        HashMap<String,String> settings = new HashMap<>();
        settings.put( "unsupported.dbms.index.spatial.curve.max_bits", Integer.toString( maxBits ) );
        for ( int i = 0; i < min.length; i++ )
        {
            char var = "xyz".toCharArray()[i];
            settings.put( crsPrefix + "." + var + ".min", Double.toString( min[i] ) );
            settings.put( crsPrefix + "." + var + ".max", Double.toString( max[i] ) );
        }
        Config config = Config.defaults();
        config.augment( settings );
        shouldGetSettingsFor( config, crs, min.length, maxBits, new Envelope( min, max ) );
    }

    private void shouldGetSettingsFor( Config config, CoordinateReferenceSystem crs, int dimensions, int maxBits, Envelope envelope )
    {
        SpaceFillingCurveSettingsFactory factory = new SpaceFillingCurveSettingsFactory( config );
        SpaceFillingCurveSettings settings = factory.settingsFor( crs );
        assertThat( "Expected " + dimensions + "D for " + crs.getName(), settings.getDimensions(), equalTo( dimensions ) );
        int maxLevels = maxBits / dimensions;
        assertThat( "Expected maxLevels=" + maxLevels + " for " + crs.getName(), settings.getMaxLevels(), equalTo( maxLevels ) );
        assertThat( "Should have normal geographic 2D extents", settings.indexExtents(), equalTo( envelope ) );
    }
}
