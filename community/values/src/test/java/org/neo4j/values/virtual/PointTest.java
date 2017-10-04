/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.values.virtual;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.neo4j.values.virtual.VirtualValueTestUtil.assertEqual;
import static org.neo4j.values.virtual.VirtualValueTestUtil.assertNotEqual;
import static org.neo4j.values.virtual.VirtualValues.pointCartesian;
import static org.neo4j.values.virtual.VirtualValues.pointGeographic;

public class PointTest
{
    @Test
    public void cartesianShouldEqualItself()
    {
        assertEqual( pointCartesian( 1.0, 2.0 ), pointCartesian( 1.0, 2.0 ) );
        assertEqual( pointCartesian( -1.0, 2.0 ), pointCartesian( -1.0, 2.0 ) );
        assertEqual( pointCartesian( -1.0, -2.0 ), pointCartesian( -1.0, -2.0 ) );
        assertEqual( pointCartesian( 0.0, 0.0 ), pointCartesian( 0.0, 0.0 ) );
    }

    @Test
    public void cartesianShouldNotEqualOtherPoint()
    {
        assertNotEqual( pointCartesian( 1.0, 2.0 ), pointCartesian( 3.0, 4.0 ) );
        assertNotEqual( pointCartesian( 1.0, 2.0 ), pointCartesian( -1.0, 2.0 ) );
    }

    @Test
    public void geographicShouldEqualItself()
    {
        assertEqual( pointGeographic( 1.0, 2.0 ), pointGeographic( 1.0, 2.0 ) );
        assertEqual( pointGeographic( -1.0, 2.0 ), pointGeographic( -1.0, 2.0 ) );
        assertEqual( pointGeographic( -1.0, -2.0 ), pointGeographic( -1.0, -2.0 ) );
        assertEqual( pointGeographic( 0.0, 0.0 ), pointGeographic( 0.0, 0.0 ) );
    }

    @Test
    public void geographicShouldNotEqualOtherPoint()
    {
        assertNotEqual( pointGeographic( 1.0, 2.0 ), pointGeographic( 3.0, 4.0 ) );
        assertNotEqual( pointGeographic( 1.0, 2.0 ), pointGeographic( -1.0, 2.0 ) );
    }

    @Test
    public void geographicShouldNotEqualCartesian()
    {
        assertNotEqual( pointGeographic( 1.0, 2.0 ), pointCartesian( 1.0, 2.0 ) );
    }

    @Test
    public void shouldHaveValueGroup()
    {
        assertTrue( pointCartesian( 1, 2 ).valueGroup() != null );
        assertTrue( pointGeographic( 1, 2 ).valueGroup() != null );
    }
}
