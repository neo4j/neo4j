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
package org.neo4j.helpers;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.collection.Iterables;

import static java.lang.Thread.currentThread;
import static java.util.Collections.singletonList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.helpers.NamedThreadFactory.named;

class ListenersTest
{
    @Test
    void copyConstructorWithNull()
    {
        assertThrows( NullPointerException.class, () -> new Listeners<>( null ) );
    }

    @Test
    void copyConstructor()
    {
        Listeners<Listener> original = newListeners( new Listener(), new Listener(), new Listener() );

        Listeners<Listener> copy = new Listeners<>( original );

        assertEquals( Iterables.asList( original ), Iterables.asList( copy ) );
    }

    @Test
    void addNull()
    {
        assertThrows( NullPointerException.class, () -> new Listeners<>().add( null ) );
    }

    @Test
    void add()
    {
        Listener[] listenersArray = {new Listener(), new Listener(), new Listener()};

        Listeners<Listener> listeners = newListeners( listenersArray );

        assertArrayEquals( listenersArray, Iterables.asArray( Listener.class, listeners ) );
    }

    @Test
    void removeNull()
    {
        assertThrows( NullPointerException.class, () -> new Listeners<>().remove( null ) );
    }

    @Test
    void remove()
    {
        Listener listener1 = new Listener();
        Listener listener2 = new Listener();
        Listener listener3 = new Listener();

        Listeners<Listener> listeners = newListeners( listener1, listener2, listener3 );

        assertEquals( Arrays.asList( listener1, listener2, listener3 ), Iterables.asList( listeners ) );

        listeners.remove( listener1 );
        assertEquals( Arrays.asList( listener2, listener3 ), Iterables.asList( listeners ) );

        listeners.remove( listener3 );
        assertEquals( singletonList( listener2 ), Iterables.asList( listeners ) );
    }

    @Test
    void notifyWithNullNotification()
    {
        assertThrows( NullPointerException.class, () -> new Listeners<>().notify( null ) );
    }

    @Test
    void notifyWithNotification()
    {
        String message = "foo";
        Listener listener1 = new Listener();
        Listener listener2 = new Listener();

        Listeners<Listener> listeners = newListeners( listener1, listener2 );

        listeners.notify( listener -> listener.process( message ) );

        assertEquals( message, listener1.message );
        assertEquals( currentThread().getName(), listener1.threadName );

        assertEquals( message, listener2.message );
        assertEquals( currentThread().getName(), listener2.threadName );
    }

    @Test
    void notifyWithNullExecutorAndNullNotification()
    {
        assertThrows( NullPointerException.class, () -> new Listeners<>().notify( null, null ) );
    }

    @Test
    void notifyWithNullExecutorAndNotification()
    {
        assertThrows( NullPointerException.class, () -> new Listeners<Listener>().notify( null, listener -> listener.process( "foo" ) ) );
    }

    @Test
    void notifyWithExecutorAndNullNotification()
    {
        assertThrows( NullPointerException.class, () -> new Listeners<Listener>().notify( newSingleThreadExecutor(), null ) );
    }

    @Test
    void notifyWithExecutorAndNotification() throws Exception
    {
        String message = "foo";
        String threadNamePrefix = "test-thread";
        Listener listener1 = new Listener();
        Listener listener2 = new Listener();

        Listeners<Listener> listeners = newListeners( listener1, listener2 );

        ExecutorService executor = newSingleThreadExecutor( named( threadNamePrefix ) );
        listeners.notify( executor, listener -> listener.process( message ) );
        executor.shutdown();
        executor.awaitTermination( 1, TimeUnit.MINUTES );

        assertEquals( message, listener1.message );
        assertThat( listener1.threadName, startsWith( threadNamePrefix ) );

        assertEquals( message, listener2.message );
        assertThat( listener2.threadName, startsWith( threadNamePrefix ) );
    }

    @Test
    void listenersIterable()
    {
        Listener listener1 = new Listener();
        Listener listener2 = new Listener();
        Listener listener3 = new Listener();

        Listeners<Listener> listeners = newListeners( listener1, listener2, listener3 );

        assertEquals( Arrays.asList( listener1, listener2, listener3 ), Iterables.asList( listeners ) );
    }

    @SafeVarargs
    private static <T> Listeners<T> newListeners( T... listeners )
    {
        Listeners<T> result = new Listeners<>();
        for ( T listener : listeners )
        {
            result.add( listener );
        }
        return result;
    }

    private static class Listener
    {
        volatile String message;
        volatile String threadName;

        void process( String message )
        {
            this.message = message;
            this.threadName = currentThread().getName();
        }
    }
}
