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

import java.util.Comparator;

import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.VirtualValue;
import org.neo4j.values.storable.NumberValues;

import static java.lang.String.format;

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
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
        writer.beginPoint( getCoordinateReferenceSystem() );
        writer.writeFloatingPoint( xCoordinate );
        writer.writeFloatingPoint( yCoordinate );
        writer.endPoint();
    }

    public abstract CoordinateReferenceSystem getCoordinateReferenceSystem();

    public double[] coordinates()
    {
        return new double[]{xCoordinate, yCoordinate};
    }

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
    public int computeHash()
    {
        int result = 0;
        result = 31 * (result + NumberValues.hash( xCoordinate ));
        result = 31 * (result + NumberValues.hash( yCoordinate ));
        return result;
    }

    @Override
    public VirtualValueGroup valueGroup()
    {
        return VirtualValueGroup.POINT;
    }

    @Override
    public int compareTo( VirtualValue other, Comparator<AnyValue> comparator )
    {
        if ( !(other instanceof PointValue) )
        {
            throw new IllegalArgumentException( "Cannot compare different virtual values" );
        }
        PointValue otherPoint = (PointValue) other;
        int x = this.getCoordinateReferenceSystem().compareTo( otherPoint.getCoordinateReferenceSystem() );

        if ( x == 0 )
        {
            x = Double.compare( xCoordinate, otherPoint.xCoordinate );

            if ( x == 0 )
            {
                return Double.compare( yCoordinate, otherPoint.yCoordinate );
            }
        }

        return x;
    }

    @Override
    public String toString()
    {
        return format( "Point{ %s, %.3e, %.3e}",
                getCoordinateReferenceSystem().name, xCoordinate, yCoordinate );
    }

    static class CartesianPointValue extends PointValue
    {

        CartesianPointValue( double x, double y )
        {
            super( x, y );
        }

        @Override
        public CoordinateReferenceSystem getCoordinateReferenceSystem()
        {
            return CoordinateReferenceSystem.Cartesian;
        }
    }

    static class GeographicPointValue extends PointValue
    {

        GeographicPointValue( double longitude, double latitude )
        {
            super( longitude, latitude );
        }

        @Override
        public CoordinateReferenceSystem getCoordinateReferenceSystem()
        {
            return CoordinateReferenceSystem.WGS84;
        }
    }
}
