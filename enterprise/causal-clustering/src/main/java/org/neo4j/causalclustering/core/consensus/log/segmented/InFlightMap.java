/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.String.format;

public class InFlightMap<V>
{
    private final SortedMap<Long,V> map = new ConcurrentSkipListMap<>();

    /**
     * Adds a new mapping.
     *
     * @param key The key of the mapping
     * @param value The value corresponding to the key provided.
     * @throws IllegalArgumentException if a mapping for the key already exists
     */
    public void put( Long key, V value )
    {
        V previousValue = map.putIfAbsent( key, value );

        if ( previousValue != null )
        {
            throw new IllegalArgumentException(
                    format( "Attempted to register an already seen value to the log entry cache. " +
                            "Key: %s; New Value: %s; Previous Value: %s", key, value, previousValue ) );
        }
    }

    /**
     * Returns the mapped value for this key or null if the key has not been registered.
     * @param key The key to use for retrieving the value from the map
     * @return the value for this key, otherwise null.
     */
    public V get( Long key )
    {
        return map.get( key );
    }

    /**
     * Attempts to remove this object from the map.
     *
     * @param key The object to attempt unregistering.
     * @return true if the attempt to unregister was successful, otherwise false if this object was not found.
     */
    public boolean remove( Long key )
    {
        return map.remove( key ) != null;
    }

    /**
     * Attempts to remove all objects at this key or higher from the map.
     *
     * @param key The object to attempt unregistering.
     */
    public void truncate( Long key )
    {
        map.tailMap( key ).keySet().forEach( map::remove );
    }

    @Override
    public String toString()
    {
        return String.format( "InFlightMap{map=%s}", map );
    }
}
