/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.helpers.collection;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility to create {@link Map}s.
 */
public abstract class MapUtil
{
    /**
     * A short-hand method for creating a {@link Map} of key/value pairs.
     * 
     * @param objects alternating key and value.
     * @param <K> type of keys
     * @param <V> type of values
     * @return a Map with the entries supplied by {@code objects}.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> genericMap( Object... objects )
    {
        Map<K, V> map = new HashMap<K, V>();
        int i = 0;
        while ( i < objects.length )
        {
            map.put( (K) objects[i++], (V) objects[i++] );
        }
        return map;
    }

    /**
     * A short-hand method for creating a {@link Map} of key/value pairs where
     * both keys and values are {@link String}s.
     * 
     * @param strings alternating key and value.
     * @return a Map with the entries supplied by {@code strings}.
     */
    public static Map<String, String> stringMap( String... strings )
    {
        return genericMap( (Object[]) strings );
    }

    /**
     * A short-hand method for creating a {@link Map} of key/value pairs where
     * keys are {@link String}s and values are {@link Object}s.
     * 
     * @param objects alternating key and value.
     * @return a Map with the entries supplied by {@code objects}.
     */
    public static Map<String, Object> map( Object... objects )
    {
        return genericMap( objects );
    }
}
