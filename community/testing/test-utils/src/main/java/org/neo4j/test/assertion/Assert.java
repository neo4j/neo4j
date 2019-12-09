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
package org.neo4j.test.assertion;

import org.awaitility.core.ConditionFactory;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.function.ThrowingAction;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.helpers.Strings;

import static java.time.Duration.ZERO;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.awaitility.Awaitility.await;

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

    public static <T> void assertEventually( Callable<T> actual, Matcher<? super T> matcher, long timeout, TimeUnit timeUnit )
    {
        assertEventually( EMPTY, actual, matcher, timeout, timeUnit );
    }

    public static <T> void assertEventually( String message, Callable<T> actual, Matcher<? super T> matcher, long timeout, TimeUnit timeUnit )
    {
        awaitCondition( message, timeout, timeUnit ).until( actual, matcher );
    }

    public static <T> void assertEventually( Supplier<String> messageSupplier, Callable<T> actual, Matcher<? super T> matcher, long timeout, TimeUnit timeUnit )
    {
        assertEventually( ignore -> messageSupplier.get(), actual, matcher, timeout, timeUnit );
    }

    public static <T> void assertEventually( Function<T,String> messageGenerator, Callable<T> actual, Matcher<? super T> matcher, long timeout,
            TimeUnit timeUnit )
    {
        awaitCondition( "await condition", timeout, timeUnit ).until( actual, new DescriptiveMatcher<>( matcher, messageGenerator ) );
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

    private static class DescriptiveMatcher<T> extends TypeSafeMatcher<T>
    {

        private final Matcher<? super T> matcher;
        private final Function<T, String> messageGenerator;
        private T lastItem;

        DescriptiveMatcher( Matcher<? super T> matcher, Function<T,String> messageGenerator )
        {
            this.matcher = matcher;
            this.messageGenerator = messageGenerator;
        }

        @Override
        protected boolean matchesSafely( T item )
        {
            this.lastItem = item;
            return matcher.matches( item );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( messageGenerator.apply( lastItem ) );
        }
    }
}
