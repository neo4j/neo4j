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
package org.neo4j.graphdb.spatial.impl;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;

/**
 * Default implementation of a {@link org.neo4j.graphdb.spatial.Point}
 */
public final class Point implements org.neo4j.graphdb.spatial.Point
{
    private final CRS crs;
    private final Coordinate coordinate;

    /**
     * Construct a point from CRS and {@link Coordinate}
     * @param crs the coordinate reference system
     * @param coordinate the coordinate object
     */
    public Point( CRS crs, Coordinate coordinate )
    {
        this.crs = crs;
        this.coordinate = coordinate;
    }

    /**
     * Construct a point from CRS and array of double coordinates
     * @param crs the coordinate reference system
     * @param coordinate the coordinates
     */
    public Point( CRS crs, double... coordinate )
    {
        this.crs = crs;
        this.coordinate = new Coordinate( coordinate );
    }

    @Override
    public List<Coordinate> getCoordinates()
    {
        return Collections.singletonList( coordinate );
    }

    @Override
    public CRS getCRS()
    {
        return crs;
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
        Point point = (Point) o;
        return Objects.equals( crs, point.crs ) && Objects.equals( coordinate, point.coordinate );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( crs, coordinate );
    }
}
