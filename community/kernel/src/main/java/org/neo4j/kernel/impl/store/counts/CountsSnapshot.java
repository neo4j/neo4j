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
package org.neo4j.kernel.impl.store.counts;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;

public class CountsSnapshot
{
    public static final CountsSnapshot NO_SNAPSHOT = null;
    final private Map<CountsKey,long[]> map;
    final private long txId;

    public CountsSnapshot( long txId, Map<CountsKey,long[]> map )
    {
        this.map = map;
        this.txId = txId;
    }

    public CountsSnapshot( long txId )
    {
        this( txId, new ConcurrentHashMap<>() );
    }

    public Map<CountsKey,long[]> getMap()
    {
        return map;
    }

    public long getTxId()
    {
        return txId;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj == this )
        {
            return true;
        }

        if ( obj == null || !(obj instanceof CountsSnapshot) )
        {
            return false;
        }

        CountsSnapshot other = (CountsSnapshot) obj;
        return other.getMap().equals( this.getMap() ) && (Long.compare( other.getTxId(), this.getTxId() ) == 0);
    }

    @Override
    public int hashCode()
    {
        int hashcode = (int) this.getTxId();
        for ( CountsKey key : map.keySet() )
        {
            hashcode += (int) key.recordType().code;
            for ( long val : map.get( key ) )
            {
                hashcode += val;
            }
        }
        return hashcode;
    }

    @Override
    public String toString()
    {
        return "CountsSnapshot{" +
                "map=" + map +
                ", txId=" + txId +
                '}';
    }
}