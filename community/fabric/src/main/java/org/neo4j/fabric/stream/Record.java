/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.fabric.stream;

import org.neo4j.values.AnyValue;

public abstract class Record
{
    public abstract AnyValue getValue( int offset );

    public abstract int size();

    @Override
    public final int hashCode()
    {
        int hashCode = 1;

        for ( var i = 0; i < size(); i++ )
        {
            hashCode = 31 * hashCode + getValue( i ).hashCode();
        }

        return hashCode;
    }

    @Override
    public final boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }

        if ( !(o instanceof Record) )
        {
            return false;
        }

        Record that = (Record) o;

        if ( this.size() != that.size() )
        {
            return false;
        }

        for ( var i = 0; i < size(); i++ )
        {
            if ( !this.getValue( i ).equals( that.getValue( i ) ) )
            {
                return false;
            }
        }

        return true;
    }
}
