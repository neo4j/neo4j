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
package org.neo4j.graphdb;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;

public class SpatialMocks
{
    public static MockPoint mockPoint( double x, double y, CRS crs )
    {
        return new MockPoint( x, y, crs );
    }

    public static MockPoint3D mockPoint( double x, double y, double z, CRS crs )
    {
        return new MockPoint3D( x, y, z, crs );
    }

    public static MockGeometry mockGeometry( String geometryType, List<Coordinate> coordinates, CRS crs )
    {
        return new MockGeometry( geometryType, coordinates, crs );
    }

    public static CRS mockWGS84()
    {
        return mockCRS( 4326, "WGS-84", "http://spatialreference.org/ref/epsg/4326/" );
    }

    public static CRS mockCartesian()
    {
        return mockCRS( 7203, "cartesian", "http://spatialreference.org/ref/sr-org/7203/" );
    }

    public static CRS mockWGS84_3D()
    {
        return mockCRS( 4979, "WGS-84-3D", "http://spatialreference.org/ref/epsg/4979/" );
    }

    public static CRS mockCartesian_3D()
    {
        return mockCRS( 9157, "cartesian-3D", "http://spatialreference.org/ref/sr-org/9157/" );
    }

    private static CRS mockCRS( final int code, final String type, final String href )
    {
        return new CRS()
        {
            public int getCode()
            {
                return code;
            }

            public String getType()
            {
                return type;
            }

            public String getHref()
            {
                return href;
            }
        };
    }

    private static class MockPoint extends MockGeometry implements Point
    {
        private final Coordinate coordinate;

        private MockPoint( final double x, final double y, final CRS crs )
        {
            super( "Point", new ArrayList<>(), crs );
            this.coordinate = new Coordinate( x, y );
            this.coordinates.add( this.coordinate );
        }
    }

    private static class MockPoint3D extends MockGeometry implements Point
    {
        private final Coordinate coordinate;

        private MockPoint3D( final double x, final double y, double z, final CRS crs )
        {
            super( "Point", new ArrayList<>(), crs );
            this.coordinate = new Coordinate( x, y, z );
            this.coordinates.add( this.coordinate );
        }
    }

    private static class MockGeometry implements Geometry
    {
        final String geometryType;
        final List<Coordinate> coordinates;
        protected final CRS crs;

        private MockGeometry( String geometryType, final List<Coordinate> coordinates, final CRS crs )
        {
            this.geometryType = geometryType;
            this.coordinates = coordinates;
            this.crs = crs;
        }

        @Override
        public String getGeometryType()
        {
            return geometryType;
        }

        @Override
        public List<Coordinate> getCoordinates()
        {
            return coordinates;
        }

        @Override
        public CRS getCRS()
        {
            return crs;
        }

        @Override
        public String toString()
        {
            return geometryType;
        }
    }
}
