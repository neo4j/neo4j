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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.utils.PrettyPrinter;
import org.neo4j.values.virtual.MapValue;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.neo4j.values.storable.ValueGroup.NUMBER;
import static org.neo4j.values.storable.ValueGroup.TEXT;

public class PointValue extends ScalarValue implements Point, Comparable<PointValue>
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

    @Override
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
            int cmpVal = Double.compare( this.coordinate[i], other.coordinate[i] );
            if ( cmpVal != 0 )
            {
                return cmpVal;
            }
        }
        return 0;
    }

    @Override
    int unsafeCompareTo( Value otherValue )
    {
        return compareTo( (PointValue) otherValue );
    }

    @Override
    Integer unsafeTernaryCompareTo( Value otherValue )
    {
        PointValue other = (PointValue) otherValue;

        if ( this.crs.getCode() != other.crs.getCode() || this.coordinate.length != other.coordinate.length )
        {
            return null;
        }

        int result = 0;
        for ( int i = 0; i < coordinate.length; i++ )
        {
            int cmpVal = Double.compare( this.coordinate[i], other.coordinate[i] );
            if ( cmpVal != 0 && cmpVal != result )
            {
                if ( (cmpVal < 0 && result > 0) || (cmpVal > 0 && result < 0) )
                {
                    return null;
                }
                result = cmpVal;
            }
        }

        return result;
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

    public static PointValue fromMap( MapValue map )
    {
        AnyValue[] fields = new Value[PointValueField.values().length];
        for ( PointValueField f : PointValueField.values() )
        {
            AnyValue fieldValue = map.get( f.name().toLowerCase() );
            fields[f.ordinal()] = fieldValue != Values.NO_VALUE ? fieldValue : null;
        }
        return fromInputFields( fields );
    }

    private static Pattern mapPattern = Pattern.compile( "\\{(.*)\\}" );
    private static Pattern keyValuePattern =
        Pattern.compile( "(?:\\A|,)\\s*+(?<k>[a-z_A-Z]\\w*+)\\s*:\\s*(?<v>[^\\s,]+)" );

    private static Pattern quotesPattern = Pattern.compile( "^[\"']|[\"']$" );

    public static PointValue parse( CharSequence text )
    {
        return PointValue.parse( text, null );
    }

    public static PointValue parse( CharSequence text, AnyValue[] fieldsFromHeader )
    {
        AnyValue[] fieldsFromData = parseIntoArray( text );
        if ( fieldsFromHeader != null )
        {
            //It is given that fieldsFromData.length == fieldsFromHeader.length because parseIntoArray produces fixed length arrays
            // Merge InputFields: Data fields override header fields
            for ( int i = 0; i < fieldsFromData.length; i++ )
            {
                if ( fieldsFromData[i] == null )
                {
                    fieldsFromData[i] = fieldsFromHeader[i];
                }
            }
        }
        return fromInputFields( fieldsFromData );
    }

    public static AnyValue[] parseIntoArray(CharSequence text)
    {
        Matcher mapMatcher = mapPattern.matcher( text );
        if ( !(mapMatcher.find() && mapMatcher.groupCount() == 1) )
        {
            String errorMessage = format( "Failed to parse point value: '%s'", text );
            throw new IllegalArgumentException( errorMessage );
        }

        String mapContents = mapMatcher.group( 1 );
        if ( mapContents.isEmpty() )
        {
            String errorMessage = format( "Failed to parse point value: '%s'", text );
            throw new IllegalArgumentException( errorMessage );
        }

        Matcher matcher = keyValuePattern.matcher( mapContents );
        if ( !(matcher.find() ) )
        {
            String errorMessage = format( "Failed to parse point value: '%s'", text );
            throw new IllegalArgumentException( errorMessage );
        }

        Value[] fields = new Value[PointValueField.values().length];

        do
        {
            String key = matcher.group( "k" );
            if ( key != null )
            {
                PointValueField field = null;
                try
                {
                    // NOTE: We let the key be case-insensitive here
                    field = PointValueField.valueOf( PointValueField.class, key.toUpperCase() );
                }
                catch ( IllegalArgumentException e )
                {
                    // Ignore unknown fields
                }

                if ( field != null )
                {
                    if ( fields[field.ordinal()] != null )
                    {
                        String errorMessage =
                                format( "Failed to parse point value: '%s'. Duplicate field '%s' is not allowed.",
                                        text, key );
                        throw new IllegalArgumentException( errorMessage );
                    }

                    String value = matcher.group( "v" );
                    if ( value != null )
                    {
                        switch ( field.valueType() )
                        {
                        case NUMBER:
                        {
                            DoubleValue doubleValue = Values.doubleValue( Double.parseDouble( value ) );
                            fields[field.ordinal()] = doubleValue;
                            break;
                        }

                        case TEXT:
                        {
                            // Eliminate any quoutes
                            String unquotedValue = quotesPattern.matcher( value ).replaceAll( "" );
                            fields[field.ordinal()] = Values.stringValue( unquotedValue );
                            break;
                        }

                        default:
                            // Just ignore unknown fields
                        }
                    }
                }
            }
        } while ( matcher.find() );

        return fields;
    }

    /**
     * This contains the logic to decide the default coordinate reference system based on the input fields
     */
    private static PointValue fromInputFields( AnyValue[] fields )
    {
        CoordinateReferenceSystem crs;
        double[] coordinates;

        AnyValue crsValue = fields[PointValueField.CRS.ordinal()];
        if ( crsValue != null )
        {
            TextValue crsName = (TextValue) crsValue;
            crs = CoordinateReferenceSystem.byName( crsName.stringValue() );
            if ( crs == null )
            {
                throw new IllegalArgumentException( "Unknown coordinate reference system: " + crsName.stringValue() );
            }
        }
        else
        {
            crs = null;
        }

        AnyValue xValue = fields[PointValueField.X.ordinal()];
        AnyValue yValue = fields[PointValueField.Y.ordinal()];
        AnyValue latitudeValue = fields[PointValueField.LATITUDE.ordinal()];
        AnyValue longitudeValue = fields[PointValueField.LONGITUDE.ordinal()];

        if ( xValue != null && yValue != null )
        {
            double x = ((NumberValue) xValue).doubleValue();
            double y = ((NumberValue) yValue).doubleValue();
            AnyValue zValue = fields[PointValueField.Z.ordinal()];

            coordinates = zValue != null ? new double[]{x, y, ((NumberValue) zValue).doubleValue()} : new double[]{x, y};
            if ( crs == null )
            {
                crs = coordinates.length == 3 ? CoordinateReferenceSystem.Cartesian_3D : CoordinateReferenceSystem.Cartesian;
            }
        }
        else if ( latitudeValue != null && longitudeValue != null )
        {
            double x = ((NumberValue) longitudeValue).doubleValue();
            double y = ((NumberValue) latitudeValue).doubleValue();
            AnyValue zValue = fields[PointValueField.Z.ordinal()];
            AnyValue heightValue = fields[PointValueField.HEIGHT.ordinal()];
            if ( zValue != null )
            {
                coordinates = new double[]{x, y, ((NumberValue) zValue).doubleValue()};
            }
            else if ( heightValue != null )
            {
                coordinates = new double[]{x, y, ((NumberValue) heightValue).doubleValue()};
            }
            else
            {
                coordinates = new double[]{x, y};
            }
            if ( crs == null )
            {
                crs = coordinates.length == 3 ? CoordinateReferenceSystem.WGS84_3D : CoordinateReferenceSystem.WGS84;
            }
            if ( !crs.isGeographic() )
            {
                throw new IllegalArgumentException( "Geographic points does not support coordinate reference system: " + crs +
                        ". This is set either in the csv header or the actual data column" );
            }
        }
        else
        {
            if ( crs == CoordinateReferenceSystem.Cartesian )
            {
                throw new IllegalArgumentException( "A " + CoordinateReferenceSystem.Cartesian.getName() + " point must contain 'x' and 'y'" );
            }
            else if ( crs == CoordinateReferenceSystem.Cartesian_3D )
            {
                throw new IllegalArgumentException( "A " + CoordinateReferenceSystem.Cartesian_3D.getName() + " point must contain 'x', 'y' and 'z'" );
            }
            else if ( crs == CoordinateReferenceSystem.WGS84 )
            {
                throw new IllegalArgumentException( "A " + CoordinateReferenceSystem.WGS84.getName() + " point must contain 'latitude' and 'longitude'" );
            }
            else if ( crs == CoordinateReferenceSystem.WGS84_3D )
            {
                throw new IllegalArgumentException( "A " + CoordinateReferenceSystem.WGS84_3D.getName() +
                                                    " point must contain 'latitude', 'longitude' and 'height'" );
            }
            throw new IllegalArgumentException( "A point must contain either 'x' and 'y' or 'latitude' and 'longitude'" );
        }

        if ( crs.getDimension() != coordinates.length )
        {
            throw new IllegalArgumentException( "Cannot create " + crs.getDimension() + "D point with " + coordinates.length + " coordinates" );
        }
        return Values.pointValue( crs, coordinates );
    }

    private enum PointValueField
    {
        X( NUMBER ),
        Y( NUMBER ),
        Z( NUMBER ),
        LATITUDE( NUMBER ),
        LONGITUDE( NUMBER ),
        HEIGHT( NUMBER ),
        CRS( TEXT );

        PointValueField( ValueGroup valueType )
        {
            this.valueType = valueType;
        }

        ValueGroup valueType()
        {
            return valueType;
        }

        private ValueGroup valueType;
    }
}
