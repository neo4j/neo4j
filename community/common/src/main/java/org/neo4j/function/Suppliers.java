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
package org.neo4j.function;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.System.currentTimeMillis;

/**
 * Constructors for basic {@link Supplier} types
 */
public final class Suppliers
{
    private Suppliers()
    {
    }

    /**
     * Creates a {@link Supplier} that returns a single object
     *
     * @param instance The object to return
     * @param <T> The object type
     * @return A {@link Supplier} returning the specified object instance
     */
    public static <T> Supplier<T> singleton( final T instance )
    {
        return () -> instance;
    }

    /**
     * Creates a lazy initialized {@link Supplier} of a single object
     *
     * @param supplier A supplier that will provide the object when required
     * @param <T> The object type
     * @return A {@link Supplier} returning the specified object instance
     */
    public static <T> Lazy<T> lazySingleton( final Supplier<T> supplier )
    {
        return new Lazy<T>()
        {
            volatile T instance;

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
            volatile V lastValue;
            T instance;

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

    public static <T, E extends Exception> ThrowingCapturingSupplier<T,E> compose(
            final ThrowingSupplier<T,? extends E> input,
            final ThrowingPredicate<T,? extends E> predicate )
    {
        return new ThrowingCapturingSupplier<>( input, predicate );
    }

    public static BooleanSupplier untilTimeExpired( long duration, TimeUnit unit )
    {
        final long endTimeInMilliseconds = currentTimeMillis() + unit.toMillis( duration );
        return () -> currentTimeMillis() <= endTimeInMilliseconds;
    }

    static class ThrowingCapturingSupplier<T, E extends Exception> implements ThrowingSupplier<Boolean,E>
    {
        private final ThrowingSupplier<T,? extends E> input;
        private final ThrowingPredicate<T,? extends E> predicate;

        private T current;

        ThrowingCapturingSupplier( ThrowingSupplier<T,? extends E> input, ThrowingPredicate<T,? extends E> predicate )
        {
            this.input = input;
            this.predicate = predicate;
        }

        T lastInput()
        {
            return current;
        }

        @Override
        public Boolean get() throws E
        {
            current = input.get();
            return predicate.test( current );
        }

        @Override
        public String toString()
        {
            return String.format( "%s on %s", predicate, input );
        }
    }

    public interface Lazy<T> extends Supplier<T>
    {
    }
}
