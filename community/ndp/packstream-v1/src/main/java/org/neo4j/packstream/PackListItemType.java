/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.packstream;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PackStream list types provide a representative marker byte for each valid PackStream
 * type, specifically for use with lists. PackStream's regular way of denoting type combines type
 * detail with other information, such as scale. Therefore, there is no precise one-to-one
 * mapping between type and marker byte values elsewhere.
 *
 * The values chosen more subtype marker bytes however do deliberately overlap with the main set
 * of markers. This should aid clarity, reduce confusion and potentially allow sharing of code
 * across both schemes.
 *
 * The full list of subtypes are:
 *
 *       Type -> Marker   (Code)
 *   ----------------------------
 *        ANY -> NULL     (C0)
 *    BOOLEAN -> TRUE     (C3)
 *    INTEGER -> INT_8    (C8)
 *      FLOAT -> FLOAT_64 (C1)
 *      BYTES -> BYTES_8  (CC)
 *       TEXT -> TEXT_8   (D0)
 *       LIST -> LIST_8   (D4)
 *        MAP -> MAP_8    (D8)
 *   ----------------------------
 *     STRUCT -> STRUCT_8 (00-7F)
 *
 */
public class PackListItemType
{
    private static final byte ANY_MARKER = PackStream.NULL;
    private static final byte BOOLEAN_MARKER = PackStream.TRUE;
    private static final byte INTEGER_MARKER = PackStream.INT_8;
    private static final byte FLOAT_MARKER = PackStream.FLOAT_64;
    private static final byte BYTES_MARKER = PackStream.BYTES_8;
    private static final byte TEXT_MARKER = PackStream.TEXT_8;
    private static final byte LIST_MARKER = PackStream.LIST_8;
    private static final byte MAP_MARKER = PackStream.MAP_8;

    private static final Map<Byte, PackListItemType> STRUCT = new HashMap<>( 0x80 );
    static
    {
        // Slightly odd loop construct but not possible to loop until Byte.MAX_VALUE
        // explicitly thanks to Java's wrap-around which means b < Byte.MAX_VALUE is
        // always true.
        for ( byte b = 0x00; b >= 0x00; b++ )
        {
            STRUCT.put( b, new PackListItemType( b, null ) );
        }
    }

    public static final PackListItemType ANY = new PackListItemType( ANY_MARKER, Object.class );
    public static final PackListItemType BOOLEAN = new PackListItemType( BOOLEAN_MARKER, Boolean.class );
    public static final PackListItemType INTEGER = new PackListItemType( INTEGER_MARKER, Long.class );
    public static final PackListItemType FLOAT = new PackListItemType( FLOAT_MARKER, Double.class );
    public static final PackListItemType BYTES = new PackListItemType( BYTES_MARKER, byte[].class );
    public static final PackListItemType TEXT = new PackListItemType( TEXT_MARKER, String.class );
    public static final PackListItemType LIST = new PackListItemType( LIST_MARKER, List.class );
    public static final PackListItemType MAP = new PackListItemType( MAP_MARKER, Map.class );

    public static PackListItemType struct( byte signature )
    {
        if ( signature >= 0x00 )
        {
            return STRUCT.get( signature );
        }
        else
        {
            throw new IllegalArgumentException(
                    "Structure signatures must be between 0x00 and 0x7F" );
        }
    }

    public static PackListItemType struct( PackStream.StructType type )
    {
        return struct( type.signature() );
    }

    public static PackListItemType fromClass( Class type )
    {
        if ( type == Object.class )
        {
            return ANY;
        }
        else if ( type == Boolean.class )
        {
            return BOOLEAN;
        }
        else if ( type == Short.class || type == Integer.class || type == Long.class )
        {
            return INTEGER;
        }
        else if ( type == Float.class || type == Double.class )
        {
            return FLOAT;
        }
        else if ( type == byte[].class )
        {
            return BYTES;
        }
        else if ( type == String.class )
        {
            return TEXT;
        }
        else if ( List.class.isAssignableFrom( type ) )
        {
            return LIST;
        }
        else if ( Map.class.isAssignableFrom( type ) )
        {
            return MAP;
        }
        else
        {
            throw new IllegalArgumentException(
                    "The class " + type.getName() + " does not have a PackStream equivalent" );
        }
    }

    public static PackListItemType fromMarkerByte( byte marker )
    {
        if ( marker >= 0x00 )
        {
            return PackListItemType.struct( marker );
        }
        else
        {
            switch ( marker )
            {
                case ANY_MARKER:
                    return ANY;
                case BOOLEAN_MARKER:
                    return BOOLEAN;
                case INTEGER_MARKER:
                    return INTEGER;
                case FLOAT_MARKER:
                    return FLOAT;
                case BYTES_MARKER:
                    return BYTES;
                case TEXT_MARKER:
                    return TEXT;
                case LIST_MARKER:
                    return LIST;
                case MAP_MARKER:
                    return MAP;
                default:
                    throw new IllegalArgumentException( "Illegal list item type marker " + marker );
            }
        }
    }

    private final byte markerByte;
    private final Class instanceClass;

    private PackListItemType( byte markerByte, Class instanceClass )
    {
        this.markerByte = markerByte;
        this.instanceClass = instanceClass;
    }

    public byte markerByte()
    {
        return markerByte;
    }

    public Class instanceClass()
    {
        return instanceClass;
    }

    public boolean isStruct()
    {
        return markerByte >= 0x00;
    }

}
