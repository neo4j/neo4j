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
package org.neo4j.function;

/**
 * Constructors for basic {@link Supplier} types
 */
public final class Suppliers
{
    /**
     * Creates a {@link Supplier} that returns a single object
     *
     * @param instance The object to return
     * @param <T> The object type
     * @return A {@link Supplier} returning the specified object instance
     */
    public static <T> Supplier<T> singleton( final T instance )
    {
        return new Supplier<T>()
        {
            @Override
            public T get()
            {
                return instance;
            }
        };
    }

    /**
     * Creates a lazy initialized {@link Supplier} of a single object
     *
     * @param supplier A supplier that will provide the object when required
     * @param <T> The object type
     * @return A {@link Supplier} returning the specified object instance
     */
    public static <T> Supplier<T> lazySingleton( final Supplier<T> supplier )
    {
        return new Supplier<T>()
        {
            volatile T instance = null;

            @Override
            public T get()
            {
                if ( instance != null )
                {
                    return instance;
                }

                synchronized ( this )
                {
                    if ( instance == null )
                    {
                        instance = supplier.get();
                    }
                }
                return instance;
            }
        };
    }

    /**
     * Creates a new {@link Supplier} that applies the specified function to the values obtained from a source supplier. The
     * function is only invoked once for every sequence of identical objects obtained from the source supplier (the previous result
     * is cached and returned again if the source object hasn't changed).
     *
     * @param supplier A supplier of source objects
     * @param adaptor A function mapping source objects to result objects
     * @param <V> The source object type
     * @param <T> The result object type
     * @return A {@link Supplier} of objects
     */
    public static <T, V> Supplier<T> adapted( final Supplier<V> supplier, final Function<V,T> adaptor )
    {
        return new Supplier<T>()
        {
            volatile V lastValue = null;
            T instance = null;

            @Override
            public T get()
            {
                V value = supplier.get();
                if ( value == lastValue )
                {
                    return instance;
                }

                T adaptedValue = adaptor.apply( value );
                synchronized ( this )
                {
                    if ( value != lastValue )
                    {
                        instance = adaptedValue;
                        lastValue = value;
                    }
                }
                return instance;
            }
        };
    }

    public static <T> Supplier<Boolean> compose( final Supplier<T> input, final Predicate<T> predicate )
    {
        return new Supplier<Boolean>()
        {
            @Override
            public Boolean get()
            {
                return predicate.test( input.get() );
            }
        };
    }
}
