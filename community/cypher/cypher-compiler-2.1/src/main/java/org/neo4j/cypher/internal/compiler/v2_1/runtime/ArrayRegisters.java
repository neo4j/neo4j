/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.runtime;

import java.util.Arrays;

public class ArrayRegisters implements Registers
{
    private final Object[] objects;
    private final long[] longs;

    public ArrayRegisters(int numObjects, int numLongs)
    {
        this( new Object[numObjects], new long[numLongs] );
    }

    public ArrayRegisters(Object[] objects, long[] longs)
    {
        this.objects = objects;
        this.longs = longs;
    }

    @Override
    public void setObjectRegister( int idx, Object value )
    {
        objects[idx] = value;
    }

    @Override
    public void setLongRegister( int idx, long value )
    {
        longs[idx] = value;
    }

    @Override
    public Object getObjectRegister( int idx )
    {
        return objects[idx];
    }

    @Override
    public long getLongRegister( int idx )
    {
        return longs[idx];
    }

    @Override
    public Registers copy()
    {
        return new ArrayRegisters( Arrays.copyOf( objects, objects.length ), Arrays.copyOf( longs, longs.length ) );
    }
}
