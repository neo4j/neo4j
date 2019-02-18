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
package org.neo4j.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.of;
import static org.neo4j.util.FeatureToggles.flag;

/**
 * A utility for locating services. This implements the same functionality as <a
 * href="https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html">
 * the Java ServiceLoader interface</a>.
 * <p>
 * Additionally this class can be used as a base class for implementing services
 * that are differentiated by a String key. An example implementation might be:
 * <pre>
 * <code>
 * public abstract class StringConverter extends org.neo4j.commons.Service
 * {
 *     protected StringConverter(String id)
 *     {
 *         super( id );
 *     }
 *
 *     public abstract String convert( String input );
 *
 *     public static StringConverter load( String id )
 *     {
 *         return org.neo4j.commons.Service.load( StringConverter.class, id );
 *     }
 * }
 * </code>
 * </pre>
 * <p>
 * With for example these implementations:
 * <pre>
 * <code>
 * public final class UppercaseConverter extends StringConverter
 * {
 *     public UppercaseConverter()
 *     {
 *         super( "uppercase" );
 *     }
 *
 *     public String convert( String input )
 *     {
 *         return input.toUpperCase();
 *     }
 * }
 *
 * public final class ReverseConverter extends StringConverter
 * {
 *     public ReverseConverter()
 *     {
 *         super( "reverse" );
 *     }
 *
 *     public String convert( String input )
 *     {
 *         char[] chars = input.toCharArray();
 *         for ( int i = 0; i &lt; chars.length/2; i++ )
 *         {
 *             char intermediate = chars[i];
 *             chars[i] = chars[chars.length-1-i];
 *             chars[chars.length-1-i] = chars[i];
 *         }
 *         return new String( chars );
 *     }
 * }
 * </code>
 * </pre>
 * <p>
 * This would then be used as:
 * <pre>
 * <code>
 * String atad = StringConverter.load( "reverse" ).convert( "data" );
 * </code>
 * </pre>
 *
 * @author Tobias Ivarsson
 */
public abstract class Service
{
    /**
     * Enabling this is useful for debugging why services aren't loaded where you would expect them to.
     */
    private static final boolean printServiceLoaderStackTraces =
            flag( Service.class, "printServiceLoaderStackTraces", false );

    private final Set<String> keys;

    /**
     * Load all implementations of a Service.
     *
     * @param type the type of the Service to load
     * @return all registered implementations of the Service
     */
    public static <T> List<T> loadAll( Class<T> type )
    {
        final Map<String, T> services = new HashMap<>();
        final ClassLoader currentCL = Service.class.getClassLoader();
        final ClassLoader contextCL = Thread.currentThread().getContextClassLoader();

        loadAllSafely( type, contextCL ).forEach( service -> services.put( service.getClass().getName(), service ) );

        // JBoss 7 does not export content of META-INF/services to context class loader, so this call adds implementations defined in Neo4j
        // libraries from the same module.
        if ( currentCL != contextCL )
        {
            // Services from context class loader have higher precedence, so we skip duplicates by comparing class names.
            loadAllSafely( type, currentCL ).forEach( service -> services.putIfAbsent( service.getClass().getName(), service ) );
        }
        return new ArrayList<>( services.values() );
    }

    /**
     * Load the Service implementation with the specified key. Matches from context class loader have highest priority.
     * If there are multiple matches within one class loader, it is not defined which one of them is returned.
     *
     * @param type the type of the Service to load
     * @param key the key that identifies the desired implementation
     * @return requested service
     */
    public static <T extends Service> Optional<T> load( Class<T> type, String key )
    {
        return loadAll( type ).stream()
                .filter( s -> s.matches( key ) )
                .findFirst();
    }

    /**
     * Load the Service implementation with the specified key. This method should never return null.
     *
     * @param type the type of the Service to load
     * @param key the key that identifies the desired implementation
     * @return the matching Service implementation
     * @throws NoSuchElementException if no service could be loaded with the given key.
     */
    public static <T extends Service> T loadOrFail( Class<T> type, String key )
    {
        return load( type, key )
                .orElseThrow( () -> new NoSuchElementException( format( "Could not find any implementation of %s with a key=\"%s\"", type.getName(), key ) ) );
    }

    private static <T> List<T> loadAllSafely( Class<T> type, ClassLoader classLoader )
    {
        final List<T> services = new ArrayList<>();
        final Iterator<T> loader = ServiceLoader.load( type, classLoader ).iterator();
        while ( loader.hasNext() )
        {
            try
            {
                services.add( loader.next() );
            }
            catch ( ServiceConfigurationError e )
            {
                if ( printServiceLoaderStackTraces )
                {
                    e.printStackTrace();
                }
            }
        }
        return services;
    }

    /**
     * Create a new instance of a service implementation identified with the
     * specified key(s).
     *
     * @param key the main key for identifying this service implementation
     * @param altKeys alternative spellings of the identifier of this service
     * implementation
     */
    protected Service( String key, String... altKeys )
    {
        if ( altKeys == null || altKeys.length == 0 )
        {
            this.keys = Collections.singleton( key );
        }
        else
        {
            this.keys = unmodifiableSet( concat( of( key ), of( altKeys ) ).collect( toSet() ) );
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSuperclass().getName() + "" + keys;
    }

    public boolean matches( String key )
    {
        return keys.contains( key );
    }

    public Set<String> getKeys()
    {
        return keys;
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

        Service service = (Service) o;
        return keys.equals( service.keys );
    }

    @Override
    public int hashCode()
    {
        return keys.hashCode();
    }
}
