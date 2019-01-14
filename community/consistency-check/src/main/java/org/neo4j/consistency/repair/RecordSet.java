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
package org.neo4j.consistency.repair;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

class RecordSet<R extends AbstractBaseRecord> implements Iterable<R>
{
    private final Map<Long, R> map = new HashMap<>();

    void add( R record )
    {
        map.put( record.getId(), record );
    }

    RecordSet<R> union( RecordSet<R> other )
    {
        RecordSet<R> set = new RecordSet<>();
        set.addAll( this );
        set.addAll( other );
        return set;
    }

    int size()
    {
        return map.size();
    }

    @Override
    public Iterator<R> iterator()
    {
        return map.values().iterator();
    }

    public void addAll( RecordSet<R> other )
    {
        for ( R record : other.map.values() )
        {
            add( record );
        }
    }

    public boolean containsAll( RecordSet<R> other )
    {
        for ( Long id : other.map.keySet() )
        {
            if ( !map.containsKey( id ) )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder( "[\n" );
        for ( R r : map.values() )
        {
            builder.append( r.toString() ).append( ",\n" );
        }
        return builder.append( "]\n" ).toString();
    }

    public static <R extends AbstractBaseRecord> RecordSet<R> asSet( R... records )
    {
        RecordSet<R> set = new RecordSet<>();
        for ( R record : records )
        {
            set.add( record );
        }
        return set;
    }
}
