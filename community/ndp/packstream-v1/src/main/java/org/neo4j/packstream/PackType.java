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

/**
 * These are the primitive types that PackStream can represent. They map to the non-graph
 * primitives of the Neo4j type system. Graph primitives and rich composite types are represented
 * as {@link #STRUCT}.
 */
public enum PackType
{
    /** The absence of a value */
    NULL,
    /** You know what this is */
    BOOLEAN,
    /** 64-bit signed integer */
    INTEGER,
    /** 64-bit floating point number */
    FLOAT,
    /** Binary data */
    BYTES,
    /** Unicode text */
    TEXT,
    /** Typed sequence of zero or more values */
    LIST,
    /** Sequence of zero or more key/value pairs, keys are unique */
    MAP,
    /** A composite data structure, made up of zero or more PackStream values and a type signature. */
    STRUCT,
    /** Undefined type, reserved for future use */
    RESERVED;

    /**
     * Maps PackStream marker bytes to types.
     */
    public static PackType fromMarkerByte( byte markerByte )
    {
        if ( markerByte >= -0x10 )
        {
            return PackType.INTEGER;
        }

        switch ( markerByte )
        {
            case PackStream.NULL:
                return PackType.NULL;
            case PackStream.TRUE:
            case PackStream.FALSE:
                return PackType.BOOLEAN;
            case PackStream.FLOAT_64:
                return PackType.FLOAT;
            case PackStream.BYTES_8:
            case PackStream.BYTES_16:
            case PackStream.BYTES_32:
                return PackType.BYTES;
            case PackStream.TEXT_8:
            case PackStream.TEXT_16:
            case PackStream.TEXT_32:
                return PackType.TEXT;
            case PackStream.LIST_8:
            case PackStream.LIST_16:
            case PackStream.LIST_32:
                return PackType.LIST;
            case PackStream.MAP_8:
            case PackStream.MAP_16:
            case PackStream.MAP_32:
                return PackType.MAP;
            case PackStream.STRUCT_8:
            case PackStream.STRUCT_16:
                return PackType.STRUCT;
            case PackStream.INT_8:
            case PackStream.INT_16:
            case PackStream.INT_32:
            case PackStream.INT_64:
                return PackType.INTEGER;
            default:
                final byte markerHighNibble = (byte) (markerByte & 0xF0);
                switch ( markerHighNibble )
                {
                    case PackStream.TINY_TEXT:
                        return PackType.TEXT;
                    case PackStream.TINY_LIST:
                        return PackType.LIST;
                    case PackStream.TINY_MAP:
                        return PackType.MAP;
                    case PackStream.TINY_STRUCT:
                        return PackType.STRUCT;
                    default:
                        return PackType.RESERVED;
                }
        }
    }

}
