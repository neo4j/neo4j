/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.String.format;

public class InFlightMap<V>
{
    private final SortedMap<Long,V> map = new ConcurrentSkipListMap<>();
    private volatile boolean enabled;

    public InFlightMap()
    {
        this ( false );
    }

    public InFlightMap( boolean enabled )
    {
        this.enabled = enabled;
    }

    public void enable()
    {
        this.enabled = true;
    }

    /**
     * Adds a new mapping.
     *
     * @param key The key of the mapping
     * @param value The value corresponding to the key provided.
     * @throws IllegalArgumentException if a mapping for the key already exists
     */
    public void put( Long key, V value )
    {
        if ( !enabled )
        {
            return;
        }

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
