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

import java.util.Iterator;
import java.util.Map;

import org.neo4j.driver.Value;

public class MapValue extends ValueAdapter
{
    private final Map<String,Value> val;

    public MapValue( Map<String,Value> val )
    {
        this.val = val;
    }

    @Override
    public boolean javaBoolean()
    {
        return !val.isEmpty();
    }

    @Override
    public long size()
    {
        return val.size();
    }

    @Override
    public Iterable<String> keys()
    {
        return val.keySet();
    }

    @Override
    public boolean isMap()
    {
        return true;
    }

    @Override
    public Iterator<Value> iterator()
    {
        final Iterator<Value> raw = val.values().iterator();
        return new Iterator<Value>()
        {
            @Override
            public boolean hasNext()
            {
                return raw.hasNext();
            }

            @Override
            public Value next()
            {
                return raw.next();
            }

            @Override
            public void remove()
            {
            }
        };
    }

    @Override
    public Value get( String key )
    {
        return val.get( key );
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

        MapValue values = (MapValue) o;

        return !(val != null ? !val.equals( values.val ) : values.val != null);

    }

    @Override
    public int hashCode()
    {
        return val != null ? val.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return String.format( "map<%s>", val.toString() );
    }
}
