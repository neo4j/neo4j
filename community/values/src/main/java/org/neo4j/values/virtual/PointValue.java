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

import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.NumberValues;
import org.neo4j.values.VirtualValue;

public abstract class PointValue extends VirtualValue
{
    private double xCoordinate;
    private double yCoordinate;

    PointValue( double x, double y )
    {
        this.xCoordinate = x;
        this.yCoordinate = y;
    }

    @Override
    public void writeTo( AnyValueWriter writer )
    {
        writer.beginPoint( getCoordinateReferenceSystem() );
        writer.writeFloatingPoint( xCoordinate );
        writer.writeFloatingPoint( yCoordinate );
        writer.endPoint();
    }

    abstract CoordinateReferenceSystem getCoordinateReferenceSystem();

    @Override
    public boolean equals( VirtualValue o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        PointValue that = (PointValue) o;
        return xCoordinate == that.xCoordinate &&
               yCoordinate == that.yCoordinate;
    }

    @Override
    public int hash()
    {
        int result = 0;
        result = 31 * ( result + NumberValues.hash( xCoordinate ) );
        result = 31 * ( result + NumberValues.hash( yCoordinate ) );
        return result;
    }

    @Override
    public VirtualValueGroup valueGroup()
    {
        return VirtualValueGroup.POINT;
    }

    static class CarthesianPointValue extends PointValue{

        CarthesianPointValue( double x, double y )
        {
            super( x, y );
        }

        @Override
        CoordinateReferenceSystem getCoordinateReferenceSystem()
        {
            return CoordinateReferenceSystem.Cartesian;
        }
    }

    static class GeographicPointValue extends PointValue{

        GeographicPointValue( double latitude, double longitude )
        {
            super( latitude, longitude );
        }

        @Override
        CoordinateReferenceSystem getCoordinateReferenceSystem()
        {
            return CoordinateReferenceSystem.WGS84;
        }
    }
}
