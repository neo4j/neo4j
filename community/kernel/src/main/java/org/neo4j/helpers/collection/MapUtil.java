/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.helpers.collection;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.helpers.Pair;

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
    public static <K, V> Map<K, V> genericMap( Object... objects )
    {
        return genericMap( new HashMap<K, V>(), objects );
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
    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> genericMap( Map<K, V> targetMap, Object... objects )
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
     * Loads a {@link Map} from a {@link Reader} assuming strings as keys
     * and values.
     *
     * @param reader the {@link Reader} containing a {@link Properties}-like
     * layout of keys and values.
     * @return the read data as a {@link Map}.
     * @throws IOException if the {@code reader} throws {@link IOException}.
     */
    public static Map<String, String> load( Reader reader ) throws IOException
    {
        Properties props = new Properties();
        props.load( reader );
        return new HashMap<String, String>( (Map) props );
    }

    /**
     * Loads a {@link Map} from a {@link Reader} assuming strings as keys
     * and values. Any {@link IOException} is wrapped and thrown as a
     * {@link RuntimeException} instead.
     *
     * @param reader the {@link Reader} containing a {@link Properties}-like
     * layout of keys and values.
     * @return the read data as a {@link Map}.
     */
    public static Map<String, String> loadStrictly( Reader reader )
    {
        try
        {
            return load( reader );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
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
    public static Map<String, String> load( InputStream stream ) throws IOException
    {
        Properties props = new Properties();
        props.load( stream );
        return new HashMap<String, String>( (Map) props );
    }

    /**
     * Loads a {@link Map} from an {@link InputStream} assuming strings as keys
     * and values. Any {@link IOException} is wrapped and thrown as a
     * {@link RuntimeException} instead.
     *
     * @param stream the {@link InputStream} containing a
     * {@link Properties}-like layout of keys and values.
     * @return the read data as a {@link Map}.
     */
    public static Map<String, String> loadStrictly( InputStream stream )
    {
        try
        {
            return load( stream );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    /**
     * Loads a {@link Map} from a {@link File} assuming strings as keys
     * and values.
     *
     * @param file the {@link File} containing a {@link Properties}-like
     * layout of keys and values.
     * @return the read data as a {@link Map}.
     * @throws IOException if the file reader throws {@link IOException}.
     */
    public static Map<String, String> load( File file ) throws IOException
    {
        FileInputStream stream = null;
        try
        {
            stream = new FileInputStream( file );
            return load( stream );
        }
        finally
        {
            closeIfNotNull( stream );
        }
    }

    private static void closeIfNotNull( Closeable closeable ) throws IOException
    {
        if ( closeable != null ) closeable.close();
    }

    /**
     * Loads a {@link Map} from a {@link File} assuming strings as keys
     * and values. Any {@link IOException} is wrapped and thrown as a
     * {@link RuntimeException} instead.
     *
     * @param file the {@link File} containing a {@link Properties}-like
     * layout of keys and values.
     * @return the read data as a {@link Map}.
     */
    public static Map<String, String> loadStrictly( File file )
    {
        try
        {
            return load( file );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
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
        OutputStream stream = null;
        try
        {
            stream = new BufferedOutputStream( new FileOutputStream( file ) );
            store( config, stream );
        }
        finally
        {
            closeIfNotNull( stream );
        }
    }

    /**
     * Stores the data in {@code config} into {@code file} in a standard java
     * {@link Properties} format. Any {@link IOException} is wrapped and thrown as a
     * {@link RuntimeException} instead.
     * @param config the data to store in the properties file.
     * @param file the file to store the properties in.
     */
    public static void storeStrictly( Map<String, String> config, File file )
    {
        try
        {
            store( config, file );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
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
     * Stores the data in {@code config} into {@code stream} in a standard java
     * {@link Properties} format. Any {@link IOException} is wrapped and thrown as a
     * {@link RuntimeException} instead.
     * @param config the data to store in the properties file.
     * @param stream the {@link OutputStream} to store the properties in.
     */
    public static void storeStrictly( Map<String, String> config, OutputStream stream )
    {
        try
        {
            store( config, stream );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
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

    /**
     * Stores the data in {@code config} into {@code writer} in a standard java
     * {@link Properties} format. Any {@link IOException} is wrapped and thrown
     * as a {@link RuntimeException} instead.
     *
     * @param config the data to store in the properties file.
     * @param writer the {@link Writer} to store the properties in.
     */
    public static void storeStrictly( Map<String, String> config, Writer writer )
    {
        try
        {
            store( config, writer );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    /**
     * Reversed a map, making the key value and the value key.
     * @param <K> the type of key in the map to reverse. These will be the
     * values in the returned map.
     * @param <V> the type of values in the map to revert. These will be the
     * keys in the returned map.
     * @param map the {@link Map} to reverse.
     * @return the reverse of {@code map}. A new {@link Map} will be returned
     * where the keys from {@code map} will be the values and the values will
     * be the keys.
     */
    public static <K, V> Map<V, K> reverse( Map<K, V> map )
    {
        Map<V, K> reversedMap = new HashMap<V, K>();
        for ( Map.Entry<K, V> entry : map.entrySet() )
        {
            reversedMap.put( entry.getValue(), entry.getKey() );
        }
        return reversedMap;
    }

    public static <K, V> Map<K, V> copyAndPut(Map<K, V> map, K key, V value)
    {
        Map<K, V> copy = new HashMap<K, V>( map );
        copy.put( key,  value);
        return copy;
    }

    public static <K, V> Map<K, V> copyAndRemove(Map<K, V> map, K key)
    {
        Map<K, V> copy = new HashMap<K, V>( map );
        copy.remove( key );
        return copy;
    }

    public static Map<String, String> toStringMap( PropertyContainer entity )
    {
        Map<String, String> out = new HashMap<>();
        for ( Map.Entry<String, Object> property : entity.getAllProperties().entrySet() )
        {
            out.put( property.getKey(), property.getValue().toString() );
        }
        return out;
    }

    public static <K,V> Map<K, V> toMap( Iterable<Pair<K, V>> pairs )
    {
        return toMap( pairs.iterator() );
    }

    public static <K,V> Map<K, V> toMap( Iterator<Pair<K, V>> pairs )
    {
        Map<K,V> result = new HashMap<K,V>();
        while(pairs.hasNext())
        {
            Pair<K,V> pair = pairs.next();
            result.put(pair.first(), pair.other());
        }
        return result;
    }

    public static <K> boolean approximatelyEqual( Map<K, Double> that, Map<K, Double> other, double tolerance)
    {
        if ( that.size() != other.size() )
        {
            return false;
        }

        for ( Map.Entry<K, Double> entry : that.entrySet() )
        {
            if ( !other.containsKey( entry.getKey() ) )
            {
                return false;
            }

            double otherValue = other.get( entry.getKey() );
            if ( Math.abs( otherValue - entry.getValue() ) > tolerance)
            {
                return false;
            }
        }

        return true;
    }

    public static <K, V> MapBuilder<K, V> entry( K key, V value )
    {
        return new MapBuilder<K, V>().entry( key, value );
    }

    public static class MapBuilder<K, V>
    {
        private Map<K, V> map = new HashMap<>();

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

}
