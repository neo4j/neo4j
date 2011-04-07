/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
            if ( stream != null )
            {
                stream.close();
            }
        }
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
}
