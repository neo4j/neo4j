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
package org.neo4j.gis.spatial.index.curves;

import org.junit.Test;

import org.neo4j.gis.spatial.index.Envelope;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public class SpaceFillingCurveConfigurationTest
{
    @Test
    public void shouldHandleMaxDepthWithEmptySearchArea()
    {
        SpaceFillingCurveConfiguration standardConfiguration = new StandardConfiguration();
        SpaceFillingCurveConfiguration partialOverlapConf = new PartialOverlapConfiguration();
        // search area is a line, thus having a search area = 0
        Envelope search = new Envelope( -180, -180, -90, 90 );
        Envelope range = new Envelope( -180, 180, -90, 90 );
        // We pad the line to a small area, but we don't expect to go deeper than level 20
        // which would take too long
        int maxLevel = 20;
        assertThat( partialOverlapConf.maxDepth( search, range, 2, 30 ), lessThan( maxLevel ) );
        assertThat( standardConfiguration.maxDepth( search, range, 2, 30 ), lessThan( maxLevel ) );
    }

    @Test
    public void shouldReturnMaxDepth1WithWholeSearchArea()
    {
        SpaceFillingCurveConfiguration standardConfiguration = new StandardConfiguration();
        SpaceFillingCurveConfiguration partialOverlapConf = new PartialOverlapConfiguration();
        // search area is a line, thus having a search area = 0
        Envelope range = new Envelope( -180, 180, -90, 90 );
        Envelope search = range;
        assertThat( partialOverlapConf.maxDepth( search, range, 2, 30 ), equalTo( 1 ) );
        assertThat( standardConfiguration.maxDepth( search, range, 2, 30 ), equalTo( 1 ) );
    }

    @Test
    public void shouldReturnMaxDepth2WithQuarterOfWholeArea()
    {
        SpaceFillingCurveConfiguration standardConfiguration = new StandardConfiguration();
        SpaceFillingCurveConfiguration partialOverlapConf = new PartialOverlapConfiguration();
        // search area is a line, thus having a search area = 0
        Envelope range = new Envelope( -180, 180, -90, 90 );
        Envelope search = new Envelope( 0, 180, 0, 90 );
        assertThat( partialOverlapConf.maxDepth( search, range, 2, 30 ), equalTo( 2 ) );
        assertThat( standardConfiguration.maxDepth( search, range, 2, 30 ), equalTo( 2 ) );
    }

    @Test
    public void shouldReturnAppropriateDepth()
    {
        final int maxLevel = 30;
        for ( int i = 0; i < maxLevel; i++ )
        {
            SpaceFillingCurveConfiguration standardConfiguration = new StandardConfiguration();
            SpaceFillingCurveConfiguration partialOverlapConf = new PartialOverlapConfiguration();
            // search area is a line, thus having a search area = 0
            Envelope range = new Envelope( 0, 1, 0, 1 );
            Envelope search = new Envelope( 0, Math.pow( 2, -i ), 0, Math.pow( 2, -i ) );
            assertThat( partialOverlapConf.maxDepth( search, range, 2, maxLevel ), equalTo( i + 1 ) );
            assertThat( standardConfiguration.maxDepth( search, range, 2, maxLevel ), equalTo( i + 1 ) );
        }
    }
}
