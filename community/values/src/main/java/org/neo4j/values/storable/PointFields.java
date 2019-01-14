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
package org.neo4j.values.storable;

import org.neo4j.values.utils.InvalidValuesArgumentException;

/**
 * Defines all valid field accessors for points
 */
public enum PointFields
{
    X( "x" )
            {
                @Override
                Value get( PointValue value )
                {
                    return value.getNthCoordinate( 0, propertyKey, false );
                }
            },
    Y( "y" )
            {
                @Override
                Value get( PointValue value )
                {
                    return value.getNthCoordinate( 1, propertyKey, false );
                }
            },
    Z( "z" )
            {
                @Override
                Value get( PointValue value )
                {
                    return value.getNthCoordinate( 2, propertyKey, false );
                }
            },
    LONGITUDE( "longitude" )
            {
                @Override
                Value get( PointValue value )
                {
                    return value.getNthCoordinate( 0, propertyKey, true );
                }
            },
    LATITUDE( "latitude" )
            {
                @Override
                Value get( PointValue value )
                {
                    return value.getNthCoordinate( 1, propertyKey, true );
                }
            },
    HEIGHT( "height" )
            {
                @Override
                Value get( PointValue value )
                {
                    return value.getNthCoordinate( 2, propertyKey, true );
                }
            },
    CRS( "crs" )
            {
                @Override
                Value get( PointValue value )
                {
                    return Values.stringValue( value.getCoordinateReferenceSystem().toString() );
                }
            },
    SRID( "srid" )
            {
                @Override
                Value get( PointValue value )
                {
                    return Values.intValue( value.getCoordinateReferenceSystem().getCode() );
                }
            };

    public String propertyKey;

    PointFields( String propertyKey )
    {
        this.propertyKey = propertyKey;
    }

    public static PointFields fromName( String fieldName )
    {
        switch ( fieldName.toLowerCase() )
        {
        case "x":
            return X;
        case "y":
            return Y;
        case "z":
            return Z;
        case "longitude":
            return LONGITUDE;
        case "latitude":
            return LATITUDE;
        case "height":
            return HEIGHT;
        case "crs":
            return CRS;
        case "srid":
            return SRID;
        default:
            throw new InvalidValuesArgumentException( "No such field: " + fieldName );
        }
    }

    abstract Value get( PointValue value );
}
