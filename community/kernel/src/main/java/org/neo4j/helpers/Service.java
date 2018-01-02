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
package org.neo4j.helpers;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.ServiceLoader;
import java.util.Set;

import org.neo4j.helpers.collection.PrefetchingIterator;

import static org.neo4j.unsafe.impl.internal.dragons.FeatureToggles.flag;

/**
 * A utility for locating services. This implements the same functionality as <a
 * href="http://java.sun.com/javase/6/docs/api/java/util/ServiceLoader.html">
 * the Java 6 ServiceLoader interface</a>, in fact it uses the
 * <code>ServiceLoader</code> if available, but backports the functionality to
 * previous Java versions and adds some error handling to ignore misconfigured
 * service implementations.
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

    /**
     * Designates that a class implements the specified service and should be
     * added to the services listings file (META-INF/services/[service-name]).
     * <p>
     * The annotation in itself does not provide any functionality for adding
     * the implementation class to the services listings file. But it serves as
     * a handle for an Annotation Processing Tool to utilize for performing that
     * task.
     *
     * @author Tobias Ivarsson
     */
    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.SOURCE)
    public @interface Implementation
    {
        /**
         * The service(s) this class implements.
         *
         * @return the services this class implements.
         */
        Class<?>[] value();
    }

    /**
     * A base class for services, similar to {@link Service}, that compares keys
     * using case insensitive comparison instead of exact comparison.
     *
     * @author Tobias Ivarsson
     */
    public static abstract class CaseInsensitiveService extends Service
    {
        /**
         * Create a new instance of a service implementation identified with the
         * specified key(s).
         *
         * @param key     the main key for identifying this service implementation
         * @param altKeys alternative spellings of the identifier of this
         *                service implementation
         */
        protected CaseInsensitiveService( String key, String... altKeys )
        {
            super( key, altKeys );
        }

        @Override
        final public boolean matches( String key )
        {
            for ( String id : keys )
            {
                if ( id.equalsIgnoreCase( key ) )
                {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Load all implementations of a Service.
     *
     * @param <T>  the type of the Service
     * @param type the type of the Service to load
     * @return all registered implementations of the Service
     */
    public static <T> Iterable<T> load( Class<T> type )
    {
        Iterable<T> loader;
        if ( null != (loader = java6Loader( type )) )
        {
            return loader;
        }
        return Collections.emptyList();
    }

    /**
     * Load the Service implementation with the specified key. This method should never return null.
     *
     * @param <T>  the type of the Service
     * @param type the type of the Service to load
     * @param key  the key that identifies the desired implementation
     * @return the matching Service implementation
     */
    public static <T extends Service> T load( Class<T> type, String key )
    {
        for ( T impl : load( type ) )
        {
            if ( impl.matches( key ) )
            {
                return impl;
            }
        }
        throw new NoSuchElementException( String.format(
                "Could not find any implementation of %s with a key=\"%s\"",
                type.getName(), key ) );
    }

    final Set<String> keys;

    /**
     * Create a new instance of a service implementation identified with the
     * specified key(s).
     *
     * @param key     the main key for identifying this service implementation
     * @param altKeys alternative spellings of the identifier of this service
     *                implementation
     */
    protected Service( String key, String... altKeys )
    {
        if ( altKeys == null || altKeys.length == 0 )
        {
            this.keys = Collections.singleton( key );
        }
        else
        {
            this.keys = new HashSet<>( Arrays.asList( altKeys ) );
            this.keys.add( key );
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

    public Iterable<String> getKeys()
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

    private static <T> Iterable<T> filterExceptions( final Iterable<T> iterable )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return new PrefetchingIterator<T>()
                {
                    final Iterator<T> iterator = iterable.iterator();

                    @Override
                    protected T fetchNextOrNull()
                    {
                        while ( iterator.hasNext() )
                        {
                            try
                            {
                                return iterator.next();
                            }
                            catch ( Throwable e )
                            {
                                if ( printServiceLoaderStackTraces )
                                {
                                    e.printStackTrace();
                                }
                            }
                        }
                        return null;
                    }
                };
            }
        };
    }

    private static <T> Iterable<T> java6Loader( Class<T> type )
    {
        try
        {
            HashMap<String, T> services = new HashMap<>();
            ClassLoader currentCL = Service.class.getClassLoader();
            ClassLoader contextCL = Thread.currentThread().getContextClassLoader();

            Iterable<T> contextClassLoaderServices = ServiceLoader.load( type, contextCL );

            if ( currentCL != contextCL )
            {
                // JBoss 7 does not export content of META-INF/services to context
                // class loader, so this call adds implementations defined in Neo4j
                // libraries from the same module.
                Iterable<T> currentClassLoaderServices = ServiceLoader.load( type, currentCL );
                // Combine services loaded by both context and module class loaders.
                // Service instances compared by full class name ( we cannot use
                // equals for instances or classes because they can came from
                // different class loaders ).
                putAllInstancesToMap( currentClassLoaderServices, services );
                // Services from context class loader have higher precedence,
                // so we load those later.
            }

            putAllInstancesToMap( contextClassLoaderServices, services );
            return services.values();
        }
        catch ( Exception | LinkageError e )
        {
            if ( printServiceLoaderStackTraces )
            {
                e.printStackTrace();
            }
            return null;
        }
    }

    private static <T> void putAllInstancesToMap( Iterable<T> services,
                                                  Map<String, T> servicesMap )
    {
        for ( T instance : filterExceptions( services ) )
        {
            if ( null != instance )
            {
                servicesMap.put( instance.getClass().getName(), instance );
            }
        }
    }
}
