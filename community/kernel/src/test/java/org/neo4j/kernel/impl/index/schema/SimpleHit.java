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
package org.neo4j.kernel.impl.index.schema;

import java.util.Objects;

import org.neo4j.index.internal.gbptree.Hit;

public class SimpleHit<KEY,VALUE> implements Hit<KEY,VALUE>
{
    private final KEY key;
    private final VALUE value;

    public SimpleHit( KEY key, VALUE value )
    {
        this.key = key;
        this.value = value;
    }

    @Override
    public KEY key()
    {
        return key;
    }

    @Override
    public VALUE value()
    {
        return value;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        @SuppressWarnings( "unchecked" )
        Hit<KEY,VALUE> simpleHit = (Hit<KEY,VALUE>) o;
        return Objects.equals( key(), simpleHit.key() ) &&
                Objects.equals( value, simpleHit.value() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( key, value );
    }

    @Override
    public String toString()
    {
        return "[" + key + "," + value + "]";
    }
}
