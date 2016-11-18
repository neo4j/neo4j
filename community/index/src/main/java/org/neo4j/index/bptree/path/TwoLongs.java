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
package org.neo4j.index.bptree.path;

public class TwoLongs
{
    public long first = -1;
    public long other = -1;

    public TwoLongs()
    {
    }

    public TwoLongs( long first, long other )
    {
        this.first = first;
        this.other = other;
    }

    @Override
    public String toString()
    {
        return "[" + first + "," + other + "]";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (first ^ (first >>> 32));
        result = prime * result + (int) (other ^ (other >>> 32));
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
            return true;
        if ( obj == null )
            return false;
        if ( getClass() != obj.getClass() )
            return false;
        TwoLongs other = (TwoLongs) obj;
        if ( first != other.first )
            return false;
        if ( this.other != other.other )
            return false;
        return true;
    }
}
