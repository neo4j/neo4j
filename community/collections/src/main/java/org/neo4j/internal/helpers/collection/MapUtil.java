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
package org.neo4j.internal.helpers.collection;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility to create {@link Map}s.
 */
public final class MapUtil
{
    private MapUtil()
    {
        // Utility class
    }
    /**
     * A short-hand method for creating a {@link Map} of key/value pairs.
     *
     * @param objects alternating key and value.
     * @param <K> type of keys
     * @param <V> type of values
     * @return a Map with the entries supplied by {@code objects}.
     */
    public static <K, V> Map<K, V> genericMap( Object... objects )
    {
        return genericMap( new HashMap<>(), objects );
    }

    /**
     * A short-hand method for adding key/value pairs into a {@link Map}.
     *
     * @param targetMap the {@link Map} to put the objects into.
     * @param objects alternating key and value.
     * @param <K> type of keys
     * @param <V> type of values
     * @return a Map with the entries supplied by {@code objects}.
     */
    @SuppressWarnings( "unchecked" )
    public static <K, V> Map<K,V> genericMap( Map<K,V> targetMap, Object... objects )
    {
        int i = 0;
        while ( i < objects.length )
        {
            targetMap.put( (K) objects[i++], (V) objects[i++] );
        }
        return targetMap;
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
     * both keys and values are {@link String}s.
     *
     * @param targetMap the {@link Map} to put the objects into.
     * @param strings alternating key and value.
     * @return a Map with the entries supplied by {@code strings}.
     */
    public static Map<String, String> stringMap( Map<String, String> targetMap,
            String... strings )
    {
        return genericMap( targetMap, (Object[]) strings );
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

    /**
     * A short-hand method for creating a {@link Map} of key/value pairs where
     * keys are {@link String}s and values are {@link Object}s.
     *
     * @param targetMap the {@link Map} to put the objects into.
     * @param objects alternating key and value.
     * @return a Map with the entries supplied by {@code objects}.
     */
    public static Map<String, Object> map( Map<String, Object> targetMap,
            Object... objects )
    {
        return genericMap( targetMap, objects );
    }

    /**
     * Loads a {@link Map} from an {@link InputStream} assuming strings as keys
     * and values.
     *
     * @param stream the {@link InputStream} containing a
     * {@link Properties}-like layout of keys and values.
     * @return the read data as a {@link Map}.
     * @throws IOException if the {@code stream} throws {@link IOException}.
     */
    public static Map<String,String> load( InputStream stream ) throws IOException
    {
        Properties props = new Properties();
        props.load( stream );

        HashMap<String,String> result = new HashMap<>();
        for ( Map.Entry<Object,Object> entry : props.entrySet() )
        {
            // Properties does not trim whitespace from the right side of values
            result.put( (String) entry.getKey(), ( (String) entry.getValue() ).trim() );
        }

        return result;
    }

    /**
     * Stores the data in {@code config} into {@code file} in a standard java
     * {@link Properties} format.
     * @param config the data to store in the properties file.
     * @param file the file to store the properties in.
     * @throws IOException IO error.
     */
    public static void store( Map<String, String> config, File file ) throws IOException
    {
        try ( OutputStream stream = new BufferedOutputStream( Files.newOutputStream( file.toPath() ) ) )
        {
            store( config, stream );
        }
    }

    /**
     * Stores the data in {@code config} into {@code stream} in a standard java
     * {@link Properties} format.
     * @param config the data to store in the properties file.
     * @param stream the {@link OutputStream} to store the properties in.
     * @throws IOException IO error.
     */
    public static void store( Map<String, String> config, OutputStream stream ) throws IOException
    {
        Properties properties = new Properties();
        for ( Map.Entry<String, String> property : config.entrySet() )
        {
            properties.setProperty( property.getKey(), property.getValue() );
        }
        properties.store( stream, null );
    }

    /**
     * Stores the data in {@code config} into {@code writer} in a standard java
     * {@link Properties} format.
     *
     * @param config the data to store in the properties file.
     * @param writer the {@link Writer} to store the properties in.
     * @throws IOException IO error.
     */
    public static void store( Map<String, String> config, Writer writer ) throws IOException
    {
        Properties properties = new Properties();
        properties.putAll( config );
        properties.store( writer, null );
    }

    public static <K, V> MapBuilder<K, V> entry( K key, V value )
    {
        return new MapBuilder<K, V>().entry( key, value );
    }

    public static class MapBuilder<K, V>
    {
        private final Map<K, V> map = new HashMap<>();

        public MapBuilder<K, V> entry( K key, V value )
        {
            map.put( key, value );
            return this;
        }

        public Map<K, V> create()
        {
            return map;
        }
    }

    /**
     * Mutates the input map by removing entries which do not have keys in the new backing data, as extracted with
     * the keyExtractor.
     * @param map the map to mutate.
     * @param newBackingData the backing data to retain.
     * @param keyExtractor the function to extract keys from the backing data.
     * @param <K> type of the key in the input map.
     * @param <V> type of the values in the input map.
     * @param <T> type of the keys in the new baking data.
     */
    public static <K, V, T> void trimToList( Map<K,V> map, List<T> newBackingData, Function<T,K> keyExtractor )
    {
        Set<K> retainedKeys = newBackingData.stream().map( keyExtractor ).collect( Collectors.toSet() );
        trimToList( map, retainedKeys );
    }

    /**
     * Mutates the input map by removing entries which do not have keys in the new backing data, as extracted with
     * the keyExtractor.
     * @param map the map to mutate.
     * @param newBackingData the backing data to retain.
     * @param keyExtractor the function to extract keys from the backing data.
     * @param <K> type of the key in the input map.
     * @param <V> type of the values in the input map.
     * @param <T> type of the keys in the new backing data.
     */
    public static <K, V, T> void trimToFlattenedList( Map<K,V> map, List<T> newBackingData,
            Function<T,Stream<K>> keyExtractor )
    {
        Set<K> retainedKeys = newBackingData.stream().flatMap( keyExtractor ).collect( Collectors.toSet() );
        trimToList( map, retainedKeys );
    }

    /**
     * Mutates the input map by removing entries which are not in the retained set of keys.
     * @param map the map to mutate.
     * @param retainedKeys the keys to retain.
     * @param <K> type of the key.
     * @param <V> type of the values.
     */
    public static <K, V> void trimToList( Map<K,V> map, Set<K> retainedKeys )
    {
        Set<K> keysToRemove = new HashSet<>( map.keySet() );
        keysToRemove.removeAll( retainedKeys );
        keysToRemove.forEach( map::remove );
    }
}
