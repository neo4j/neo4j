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
package org.neo4j.register;

public class Registers
{
    public static Register.LongRegister newLongRegister() { return newLongRegister( -1 ); }
    public static Register.LongRegister newLongRegister(final long initialValue)
    {
        return new Register.LongRegister()
        {
            private long value = initialValue;
            @Override
            public long read()
            {
                return value;
            }

            @Override
            public void write( long value )
            {
                this.value = value;
            }
        };
    }

    public static Register.IntRegister newIntRegister() { return newIntRegister( -1 ); }
    public static Register.IntRegister newIntRegister(final int initialValue)
    {
        return new Register.IntRegister()
        {
            private int value = initialValue;
            @Override
            public int read()
            {
                return value;
            }

            @Override
            public void write( int value )
            {
                this.value = value;
            }
        };
    }

    public static <T> Register.ObjectRegister<T> newObjectRegister() { return newObjectRegister( null ); }
    public static <T> Register.ObjectRegister<T> newObjectRegister(final T initialValue)
    {
        return new Register.ObjectRegister<T>()
        {
            private T value = initialValue;
            @Override
            public T read()
            {
                return value;
            }

            @Override
            public void write( T value )
            {
                this.value = value;
            }
        };
    }
}
