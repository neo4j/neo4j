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
package org.neo4j.graphdb.spatial;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;

/**
 * A coordinate is used to describe a position in space.
 * <p>
 * A coordinate is described by at least two numbers and must adhere to the following ordering
 * <ul>
 * <li>x, y, z ordering in a cartesian reference system</li>
 * <li>east, north, altitude in a projected coordinate reference system</li>
 * <li>longitude, latitude, altitude in a geographic reference system</li>
 * </ul>
 * <p>
 * Additional numbers are allowed and the meaning of these additional numbers depends on the coordinate reference
 * system
 * (see ${@link CRS})
 */
public final class Coordinate
{
    private final double[] coordinate;

    public Coordinate( double... coordinate )
    {
        if ( coordinate.length < 2 )
        {
            throw new IllegalArgumentException( "A coordinate must have at least two elements" );
        }
        this.coordinate = coordinate;
    }

    /**
     * Returns the current coordinate.
     *
     * @return A list of numbers describing the coordinate.
     */
    public List<Double> getCoordinate()
    {
        return stream( coordinate ).boxed().collect( Collectors.toList() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        Coordinate that = (Coordinate) o;

        return Arrays.equals( coordinate, that.coordinate );

    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( coordinate );
    }
}

