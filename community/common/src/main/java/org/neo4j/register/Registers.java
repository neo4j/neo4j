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
package org.neo4j.register;

public class Registers
{
    private Registers()
    {
    }

    public static Register.DoubleLongRegister newDoubleLongRegister()
    {
        return newDoubleLongRegister( -1L, -1L );
    }

    public static Register.DoubleLongRegister newDoubleLongRegister( final long initialFirst, final long initialSecond )
    {
        return new Register.DoubleLongRegister()
        {
            private long first = initialFirst;
            private long second = initialSecond;

            @Override
            public long readFirst()
            {
                return first;
            }

            @Override
            public long readSecond()
            {
                return second;
            }

            @Override
            public void copyTo( Register.DoubleLong.Out target )
            {
                target.write( first, second );
            }

            @Override
            public boolean hasValues( long first, long second )
            {
                return this.first == first && this.second == second;
            }

            @Override
            public void write( long first, long second )
            {
                this.first = first;
                this.second = second;
            }

            @Override
            public void increment( long firstDelta, long secondDelta )
            {
                this.first += firstDelta;
                this.second += secondDelta;
            }

            @Override
            public String toString()
            {
                return "DoubleLongRegister{first=" + first + ", second=" + second + "}";
            }
        };
    }
}
