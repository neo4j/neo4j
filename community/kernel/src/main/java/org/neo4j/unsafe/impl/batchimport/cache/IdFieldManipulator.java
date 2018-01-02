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
package org.neo4j.unsafe.impl.batchimport.cache;

/**
 * Manipulator of node cache fields containing relationship id and relationship count.
 */
public class IdFieldManipulator
{
    private static final LongBitsManipulator MANIPULATOR = new LongBitsManipulator( 64-29 /*id*/, 29/*count*/ );
    private static final long EMPTY_FIELD = MANIPULATOR.template( true, false );

    private IdFieldManipulator()
    {
    }

    public static long setId( long field, long id )
    {
        return MANIPULATOR.set( field, 0, id );
    }

    public static long getId( long field )
    {
        return MANIPULATOR.get( field, 0 );
    }

    public static long changeCount( long field, int diff )
    {
        return setCount( field, getCount( field )+diff );
    }

    public static int getCount( long field )
    {
        return (int) MANIPULATOR.get( field, 1 );
    }

    public static long setCount( long field, int count )
    {
        return MANIPULATOR.set( field, 1, count );
    }

    public static long cleanId( long field )
    {
        return MANIPULATOR.clear( field, 0, true );
    }

    public static long emptyField()
    {
        return EMPTY_FIELD;
    }
}
