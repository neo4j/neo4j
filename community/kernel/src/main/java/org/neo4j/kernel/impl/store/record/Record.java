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
package org.neo4j.kernel.impl.store.record;

/**
 * Various constants used in records for different stores.
 */
public enum Record
{
    /**
     * Generic value of a reference not pointing to anything.
     */
    NULL_REFERENCE( (byte) -1, -1 ),

    NOT_IN_USE( (byte) 0, 0 ),
    IN_USE( (byte) 1, 1 ),
    FIRST_IN_CHAIN( (byte) 2, 2 ),
    RESERVED( (byte) -1, -1 ),
    NO_NEXT_PROPERTY( NULL_REFERENCE ),
    NO_PREVIOUS_PROPERTY( NULL_REFERENCE ),
    NO_NEXT_RELATIONSHIP( NULL_REFERENCE ),
    NO_PREV_RELATIONSHIP( NULL_REFERENCE ),
    NO_NEXT_BLOCK( NULL_REFERENCE ),

    NODE_PROPERTY( (byte) 0, 0 ),
    REL_PROPERTY( (byte) 2, 2 ),

    NO_LABELS_FIELD( (byte)0, 0 );

    public static final byte CREATED_IN_TX = 2;

    private byte byteValue;
    private int intValue;

    private Record( Record from )
    {
        this( from.byteValue, from.intValue );
    }

    private Record( byte byteValue, int intValue )
    {
        this.byteValue = byteValue;
        this.intValue = intValue;
    }

    /**
     * Returns a byte value representation for this record type.
     *
     * @return The byte value for this record type
     */
    public byte byteValue()
    {
        return byteValue;
    }

    /**
     * Returns a int value representation for this record type.
     *
     * @return The int value for this record type
     */
    public int intValue()
    {
        return intValue;
    }

    public boolean is( long value )
    {
        return value == intValue;
    }
}
