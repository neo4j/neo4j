/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.test.assertion;

import org.assertj.core.api.Condition;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.function.Executable;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.function.Suppliers;
import org.neo4j.function.ThrowingAction;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.helpers.Strings;

import static java.time.Duration.ZERO;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.test.conditions.Conditions.condition;

public final class Assert
{
    private Assert()
    {
    }

    public static <E extends Exception> void awaitUntilAsserted( ThrowingAction<E> condition )
    {
        awaitUntilAsserted( null, condition );
    }

    public static <E extends Exception> void awaitUntilAsserted( String alias, ThrowingAction<E> condition )
    {
        await( alias )
                .atMost( 1, MINUTES )
                .pollDelay( ZERO )
                .pollInterval( 50, MILLISECONDS )
                .pollInSameThread()
                .untilAsserted( condition::apply );
    }

    public static void assertObjectOrArrayEquals( Object expected, Object actual )
    {
        assertObjectOrArrayEquals( "", expected, actual );
    }

    public static void assertObjectOrArrayEquals( String message, Object expected, Object actual )
    {
        if ( expected.getClass().isArray() )
        {
            if ( !ArrayUtil.equals( expected, actual ) )
            {
                throw newAssertionError( message, expected, actual );
            }
        }
        else
        {
            if ( !Objects.equals( expected, actual ) )
            {
                throw newAssertionError( message, expected, actual );
            }
        }
    }

    public static <T> void assertEventually( Callable<T> actual, Predicate<? super T> predicate, long timeout, TimeUnit timeUnit )
    {
        assertEventually( EMPTY, actual, condition( predicate ), timeout, timeUnit );
    }

    public static <T> void assertEventually( Callable<T> actual, Condition<? super T> condition, long timeout, TimeUnit timeUnit )
    {
        assertEventually( EMPTY, actual, condition, timeout, timeUnit );
    }

    public static <T> void assertEventually( String message, Callable<T> actual, Condition<? super T> condition, long timeout, TimeUnit timeUnit )
    {
        awaitCondition( message, timeout, timeUnit ).untilAsserted( () -> assertThat( actual.call() ).satisfies( condition ) );
    }

    public static <T> void assertEventually( String message, Callable<T> actual, Predicate<? super T> predicate, long timeout, TimeUnit timeUnit )
    {
        awaitCondition( message, timeout, timeUnit ).untilAsserted( () -> assertThat( actual.call() ).satisfies( condition( predicate ) ) );
    }

    public static <T> void assertEventually( Supplier<String> messageSupplier, Callable<T> actual, Condition<? super T> condition, long timeout,
            TimeUnit timeUnit )
    {
        assertEventually( ignore -> messageSupplier.get(), actual, condition, timeout, timeUnit );
    }

    public static <T> void assertEventually( Function<T,String> messageGenerator, Callable<T> actual, Condition<? super T> condition, long timeout,
            TimeUnit timeUnit )
    {
        awaitCondition( "await condition", timeout, timeUnit ).untilAsserted( () -> {
            var value = actual.call();
            assertThat( value ).as( messageGenerator.apply( value ) ).satisfies( condition );
        } );
    }

    public static <T extends Throwable> void assertEventuallyThrows(
            String message, Class<T> expectedType, Executable actual, long timeout, TimeUnit timeUnit )
    {
        assertEventuallyThrows( Suppliers.singleton( message ), expectedType, actual, timeout, timeUnit );
    }

    public static <T extends Throwable> void assertEventuallyThrows(
            Supplier<String> messageGenerator, Class<T> expectedType, Executable actual, long timeout, TimeUnit timeUnit )
    {
        awaitCondition( "should throw", timeout, timeUnit ).untilAsserted( () -> assertThrows( expectedType, actual, messageGenerator ) );
    }

    private static ConditionFactory awaitCondition( String alias, long timeout, TimeUnit timeUnit )
    {
        return await( alias ).atMost( timeout, timeUnit )
                .pollDelay( 10, MILLISECONDS ).pollInSameThread();
    }

    private static AssertionError newAssertionError( String message, Object expected, Object actual )
    {
        return new AssertionError( ((message == null || message.isEmpty()) ? "" : message + "\n") +
                                   "Expected: " + Strings.prettyPrint( expected ) +
                                   ", actual: " + Strings.prettyPrint( actual ) );
    }
}
