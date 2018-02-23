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
package org.neo4j.values.storable;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;

public class CoordinateReferenceSystemTest
{
    @Test
    public void shouldGetCrsByCode()
    {
        assertEquals( Cartesian, CoordinateReferenceSystem.get( Cartesian.getCode() ) );
        assertEquals( WGS84, CoordinateReferenceSystem.get( WGS84.getCode() ) );
    }

    @Test
    public void shouldFailToGetWithIncorrectCode()
    {
        try
        {
            CoordinateReferenceSystem.get( 42 );
            fail( "Exception expected" );
        }
        catch ( IllegalArgumentException e )
        {
            assertEquals( "Unknown CRS code: 42", e.getMessage() );
        }
    }
}
