/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.internal.value;

import java.util.Arrays;
import java.util.Iterator;

import org.neo4j.driver.Value;

public class ListValue extends ValueAdapter
{
    private final Value[] values;

    public ListValue( Value... values )
    {
        this.values = values;
    }

    @Override
    public boolean javaBoolean()
    {
        return values.length > 0;
    }

    @Override
    public boolean isList()
    {
        return true;
    }

    @Override
    public long size()
    {
        return values.length;
    }

    @Override
    public Value get( long index )
    {
        return values[(int) index];
    }

    @Override
    public Iterator<Value> iterator()
    {
        return new Iterator<Value>()
        {
            private int cursor = 0;

            @Override
            public boolean hasNext()
            {
                return cursor < values.length;
            }

            @Override
            public Value next()
            {
                return values[cursor++];
            }

            @Override
            public void remove()
            {
            }
        };
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

        ListValue values1 = (ListValue) o;

        return Arrays.equals( values, values1.values );

    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( values );
    }

    @Override
    public String toString()
    {
        return "ListValue[" + Arrays.toString( values ) + "]";
    }
}
