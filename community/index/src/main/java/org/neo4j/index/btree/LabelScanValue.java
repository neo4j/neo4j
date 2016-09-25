/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.btree;

public class LabelScanValue
{
    public long bits;

    public boolean get( int bit )
    {
        assert bit >= 0 && bit < Long.SIZE;
        return (bits & mask( bit )) != 0;
    }

    private long mask( int bit )
    {
        return 1L << bit;
    }

    public void set( int bit, boolean value )
    {
        assert bit >= 0 && bit < Long.SIZE;
        if ( value )
        {
            bits |= mask( bit );
        }
        else
        {
            bits &= ~mask( bit );
        }
    }

    @Override
    public String toString()
    {
        return String.valueOf( bits );
    }
}
