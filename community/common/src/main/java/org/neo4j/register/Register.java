/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.register;

/**
 * Collection of common register types.
 */
public interface Register
{
    interface CopyableDoubleLongRegister extends DoubleLong.Copyable, DoubleLong.Out
    {
    }

    interface DoubleLongRegister extends DoubleLong.In, CopyableDoubleLongRegister
    {
    }

    interface DoubleLong
    {
        interface In
        {
            long readFirst();

            long readSecond();
        }

        interface Copyable
        {
            void copyTo( DoubleLong.Out target );

            boolean hasValues( long first, long second );
        }

        interface Out
        {
            void write( long first, long second );

            void increment( long firstDelta, long secondDelta );
        }
    }

    interface Long
    {
        interface In
        {
            long read();
        }

        interface Out
        {
            void write( long value );

            long increment( long delta );
        }
    }

    interface Int
    {
        interface In
        {
            int read();
        }

        interface Out
        {
            void write( int value );

            int increment( int delta );
        }
    }

    interface Object
    {
        interface In<T>
        {
            T read();
        }

        interface Out<T>
        {
            void write( T value );
        }

    }
}
