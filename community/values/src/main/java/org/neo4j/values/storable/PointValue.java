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

import java.util.Arrays;
import java.util.List;

import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.utils.PrettyPrinter;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

public class PointValue extends ScalarValue implements Comparable<PointValue>, Point
{
    private CoordinateReferenceSystem crs;
    private double[] coordinate;

    PointValue( CoordinateReferenceSystem crs, double... coordinate )
    {
        this.crs = crs;
        this.coordinate = coordinate;
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.writePoint( getCoordinateReferenceSystem(), coordinate );
    }

    @Override
    public String prettyPrint()
    {
        PrettyPrinter prettyPrinter = new PrettyPrinter();
        this.writeTo( prettyPrinter );
        return prettyPrinter.value();
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.GEOMETRY;
    }

    @Override
    public NumberType numberType()
    {
        return NumberType.NO_NUMBER;
    }

    @Override
    public boolean equals( Value other )
    {
        if ( other instanceof PointValue )
        {
            PointValue pv = (PointValue) other;
            return Arrays.equals( this.coordinate, pv.coordinate ) && this.getCoordinateReferenceSystem().equals( pv.getCoordinateReferenceSystem() );
        }
        return false;
    }

    public boolean equals( Point other )
    {
        if ( !other.getCRS().getHref().equals( this.getCRS().getHref() ) )
        {
            return false;
        }
        List<Double> otherCoordinate = other.getCoordinate().getCoordinate();
        if ( otherCoordinate.size() != this.coordinate.length )
        {
            return false;
        }
        for ( int i = 0; i < this.coordinate.length; i++ )
        {
            if ( otherCoordinate.get( i ) != this.coordinate[i] )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean eq( Object other )
    {
        return other != null && ((other instanceof Value && equals( (Value) other )) || (other instanceof Point && equals( (Point) other )));
    }

    public int compareTo( PointValue other )
    {
        int cmpCRS = this.crs.getCode() - other.crs.getCode();
        if ( cmpCRS != 0 )
        {
            return cmpCRS;
        }

        if ( this.coordinate.length > other.coordinate.length )
        {
            return 1;
        }
        else if ( this.coordinate.length < other.coordinate.length )
        {
            return -1;
        }

        for ( int i = 0; i < coordinate.length; i++ )
        {
            int cmpVal = Double.compare(this.coordinate[i], other.coordinate[i]);
            if ( cmpVal != 0 )
            {
                return cmpVal;
            }
        }
        return 0;
    }

    @Override
    public Point asObjectCopy()
    {
        return this;
    }

    public CoordinateReferenceSystem getCoordinateReferenceSystem()
    {
        return crs;
    }

    /*
     * Consumers must not modify the returned array.
     */
    public double[] coordinate()
    {
        return this.coordinate;
    }

    @Override
    public int computeHash()
    {
        int result = 1;
        result = 31 * result + NumberValues.hash( crs.getCode() );
        result = 31 * result + NumberValues.hash( coordinate );
        return result;
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapPoint( this );
    }

    @Override
    public String toString()
    {
        return format( "Point{ %s, %s}", getCoordinateReferenceSystem().getName(), Arrays.toString( coordinate ) );
    }

    @Override
    public List<Coordinate> getCoordinates()
    {
        return singletonList( new Coordinate( coordinate ) );
    }

    @Override
    public CRS getCRS()
    {
        return crs;
    }

    public boolean withinRange( PointValue lower, boolean includeLower, PointValue upper, boolean includeUpper )
    {
        boolean checkLower = lower != null;
        boolean checkUpper = upper != null;

        if ( checkLower && this.crs.getCode() != lower.crs.getCode() )
        {
            return false;
        }
        if ( checkUpper && this.crs.getCode() != upper.crs.getCode() )
        {
            return false;
        }

        for ( int i = 0; i < coordinate.length; i++ )
        {
            if ( checkLower )
            {
                int compareLower = Double.compare( this.coordinate[i], lower.coordinate[i] );
                if ( compareLower < 0 || compareLower == 0 && !includeLower )
                {
                    return false;
                }
            }
            if ( checkUpper )
            {
                int compareUpper = Double.compare( this.coordinate[i], upper.coordinate[i] );
                if ( compareUpper > 0 || compareUpper == 0 && !includeUpper )
                {
                    return false;
                }
            }
        }
        return true;
    }
}
