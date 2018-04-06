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
package org.neo4j.gis.spatial.index.curves;

import org.junit.Test;

import org.neo4j.gis.spatial.index.Envelope;

import static org.hamcrest.Matchers.equalTo;
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
        int maxLevel = 30;
        assertThat( partialOverlapConf.maxDepth( search, range, 2, maxLevel ), equalTo( maxLevel ) );
        assertThat( standardConfiguration.maxDepth( search, range, 2, maxLevel ), equalTo( maxLevel ) );
    }

    @Test
    public void shouldReturnMaxLevelForSmallSearchArea()
    {
        SpaceFillingCurveConfiguration standardConfiguration = new StandardConfiguration();
        // This isn't a valid envelope, just used to get a large but still finite searchRatio
        Envelope search = new Envelope( 0, Double.MIN_VALUE, 0, 100000000000000000000.0 );
        Envelope range = new Envelope( -180, 180, -90, 90 );
        int maxLevel = 30;
        assertThat( standardConfiguration.maxDepth( search, range, 2, maxLevel ), equalTo( maxLevel ) );
    }
}
