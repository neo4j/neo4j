package org.neo4j.commons.collection;

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
     * @param string alternating key and value.
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
