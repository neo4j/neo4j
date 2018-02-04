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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import static java.lang.Thread.currentThread;
import static java.util.Collections.singletonList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.helpers.NamedThreadFactory.named;
import static org.neo4j.helpers.collection.Iterables.asArray;
import static org.neo4j.helpers.collection.Iterables.asList;

public class ListenersTest
{
    @Test
    public void copyConstructorWithNull()
    {
        assertThrows( NullPointerException.class, () -> {
            new Listeners<>( null );
        } );
    }

    @Test
    public void copyConstructor()
    {
        Listeners<Listener> original = newListeners( new Listener(), new Listener(), new Listener() );

        Listeners<Listener> copy = new Listeners<>( original );

        assertEquals( asList( original ), asList( copy ) );
    }

    @Test
    public void addNull()
    {
        assertThrows( NullPointerException.class, () -> {
            new Listeners<>().add( null );
        } );
    }

    @Test
    public void add()
    {
        Listener[] listenersArray = {new Listener(), new Listener(), new Listener()};

        Listeners<Listener> listeners = newListeners( listenersArray );

        assertArrayEquals( listenersArray, asArray( Listener.class, listeners ) );
    }

    @Test
    public void removeNull()
    {
        assertThrows( NullPointerException.class, () -> {
            new Listeners<>().remove( null );
        } );
    }

    @Test
    public void remove()
    {
        Listener listener1 = new Listener();
        Listener listener2 = new Listener();
        Listener listener3 = new Listener();

        Listeners<Listener> listeners = newListeners( listener1, listener2, listener3 );

        assertEquals( Arrays.asList( listener1, listener2, listener3 ), asList( listeners ) );

        listeners.remove( listener1 );
        assertEquals( Arrays.asList( listener2, listener3 ), asList( listeners ) );

        listeners.remove( listener3 );
        assertEquals( singletonList( listener2 ), asList( listeners ) );
    }

    @Test
    public void notifyWithNullNotification()
    {
        assertThrows( NullPointerException.class, () -> {
            new Listeners<>().notify( null );
        } );
    }

    @Test
    public void notifyWithNotification()
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
    public void notifyWithNullExecutorAndNullNotification()
    {
        assertThrows( NullPointerException.class, () -> {
            new Listeners<>().notify( null, null );
        } );
    }

    @Test
    public void notifyWithNullExecutorAndNotification()
    {
        assertThrows( NullPointerException.class, () -> {
            new Listeners<Listener>().notify( null, listener -> listener.process( "foo" ) );
        } );
    }

    @Test
    public void notifyWithExecutorAndNullNotification()
    {
        assertThrows( NullPointerException.class, () -> {
            new Listeners<Listener>().notify( newSingleThreadExecutor(), null );
        } );
    }

    @Test
    public void notifyWithExecutorAndNotification() throws Exception
    {
        String message = "foo";
        String threadNamePrefix = "test-thread";
        Listener listener1 = new Listener();
        Listener listener2 = new Listener();

        Listeners<Listener> listeners = newListeners( listener1, listener2 );

        ExecutorService executor = newSingleThreadExecutor( named( threadNamePrefix ) );
        listeners.notify( executor, listener -> listener.process( message ) );
        executor.shutdown();
        executor.awaitTermination( 1, MINUTES );

        assertEquals( message, listener1.message );
        assertThat( listener1.threadName, startsWith( threadNamePrefix ) );

        assertEquals( message, listener2.message );
        assertThat( listener2.threadName, startsWith( threadNamePrefix ) );
    }

    @Test
    public void listenersIterable()
    {
        Listener listener1 = new Listener();
        Listener listener2 = new Listener();
        Listener listener3 = new Listener();

        Listeners<Listener> listeners = newListeners( listener1, listener2, listener3 );

        assertEquals( Arrays.asList( listener1, listener2, listener3 ), asList( listeners ) );
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
