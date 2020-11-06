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
package org.neo4j.logging;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractThrowableAssert;

import java.util.Arrays;

import org.neo4j.logging.AssertableLogProvider.LogCall;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.Exceptions.stringify;

public class LogAssert extends AbstractAssert<LogAssert, AssertableLogProvider>
{
    private Class<?> loggerClazz;
    private AssertableLogProvider.Level logLevel;

    public LogAssert( AssertableLogProvider logProvider )
    {
        super( logProvider, LogAssert.class );
    }

    public LogAssert forClass( Class<?> clazz )
    {
        loggerClazz = clazz;
        return this;
    }

    public LogAssert forLevel( AssertableLogProvider.Level level )
    {
        this.logLevel = level;
        return this;
    }

    public LogAssert containsMessages( String... messages )
    {
        isNotNull();
        for ( String message : messages )
        {
            if ( !haveMessage( message ) )
            {
                failWithMessage( "Expected log to contain messages: `%s` but no matches found in:%n%s", Arrays.toString( messages ), actual.serialize() );
            }
        }
        return this;
    }

    public LogAssert containsMessagesOnce( String... messages )
    {
        isNotNull();
        for ( String message : messages )
        {
            long messageMatchCount = messageMatchCount( message );
            if ( messageMatchCount != 1 )
            {
                if ( messageMatchCount == 0 )
                {
                    failWithMessage( "Expected log to contain messages: `%s` exactly once but no matches found in:%n%s",
                            Arrays.toString( messages ), actual.serialize() );
                }
                else
                {
                    failWithMessage( "Expected log to contain messages: `%s` exactly once but %d matches found in:%n%s",
                            Arrays.toString( messages ), messageMatchCount, actual.serialize() );
                }
            }
        }
        return this;
    }

    public LogAssert doesNotHaveAnyLogs()
    {
        isNotNull();
        if ( actual.getLogCalls().stream()
                   .anyMatch(call -> matchedLogger( call ) && matchedLevel( call ) ) )
        {
            failWithMessage( "Expected log to be empty but following log calls were recorded:%n%s", actual.serialize() );
        }
        return this;
    }

    public LogAssert doesNotContainMessage( String message )
    {
        isNotNull();
        if ( haveMessage( message ) )
        {
            failWithMessage( "Unexpected log message: `%s` in:%n%s", message, actual.serialize() );
        }
        return this;
    }

    public LogAssert containsMessageWithArguments( String message, Object... arguments )
    {
        isNotNull();
        if ( !haveMessageWithArguments( message, arguments ) )
        {
            failWithMessage( "Expected log to contain messages: `%s` with arguments: `%s`. " +
                            "But no matches found in:%n%s", message, Arrays.toString( arguments ), actual.serialize() );
        }
        return this;
    }

    public LogAssert doesNotContainMessageWithArguments( String message, Object... arguments )
    {
        isNotNull();
        if ( haveMessageWithArguments( message, arguments ) )
        {
            failWithMessage( "Unexpected log message: `%s` with arguments: `%s` " +
                    " in:%n%s", message, Arrays.toString( arguments ), actual.serialize() );
        }
        return this;
    }

    public LogAssert eachMessageContains( String message )
    {
        isNotNull();
        for ( LogCall logCall : actual.getLogCalls() )
        {
            if ( !matchedMessage( message, logCall ) )
            {
                failWithMessage( "Expected each log message to contain '%s', but message '%s' doesn't", message, logCall.toLogLikeString() );
            }
        }
        return this;
    }

    public AbstractThrowableAssert<?,? extends Throwable> assertExceptionForLogMessage( String message )
    {
        isNotNull();
        haveMessage( message );
        var logCall = actual.getLogCalls().stream()
                .filter( call -> matchedLogger( call ) && matchedLevel( call ) && matchedMessage( message, call ) ).findFirst();
        if ( logCall.isEmpty() )
        {
            failWithMessage( "Expected log call with message `%s` not found in:%n%s.", message, actual.serialize() );
        }
        return assertThat( logCall.get().getThrowable() );
    }

    public LogAssert containsMessageWithException( String message, Throwable t )
    {
        isNotNull();
        requireNonNull( t );
        if ( !haveMessageWithException( message, t ) )
        {
            failWithMessage( "Expected log to contain message `%s` with exception: `%s`. But no matches found in:%n%s",
                    message, stringify( t ), actual.serialize() );
        }
        return this;
    }

    public LogAssert containsException( Throwable t )
    {
        requireNonNull( t );
        isNotNull();
        if ( actual.getLogCalls().stream().noneMatch( call -> matchedLogger( call ) &&
                                                    matchedLevel( call ) &&
                                                    t.equals( call.getThrowable() ) ) )
        {
            failWithMessage( "Expected log to contain exception: `%s`. But no matches found in:%n%s",
                    stringify( t ), actual.serialize() );
        }
        return this;
    }

    private boolean haveMessageWithException( String message, Throwable t )
    {
        return actual.getLogCalls().stream().anyMatch(
                call -> matchedLogger( call ) && matchedLevel( call ) &&
                        t.equals( call.getThrowable() ) && matchedMessage( message, call ) );
    }

    private boolean haveMessageWithArguments( String message, Object... arguments )
    {
        var logCalls = actual.getLogCalls();
        return logCalls.stream().anyMatch( call -> matchedLogger( call )
                && matchedLevel( call ) && Arrays.equals( call.getArguments(), arguments ) &&
                matchedMessage( message, call ) );
    }

    private boolean haveMessage( String message )
    {
        var logCalls = actual.getLogCalls();
        return logCalls.stream().anyMatch( call -> matchedLogger( call ) &&
                matchedLevel( call ) &&
                matchedMessage( message, call ) );
    }

    private long messageMatchCount( String message )
    {
        var logCalls = actual.getLogCalls();
        return logCalls.stream().filter( call -> matchedLogger( call ) &&
                matchedLevel( call ) &&
                matchedMessage( message, call ) ).count();
    }

    private static boolean matchedMessage( String message, LogCall call )
    {
        return call.getMessage().contains( message ) || call.toLogLikeString().contains( message );
    }

    private boolean matchedLogger( LogCall call )
    {
        return loggerClazz == null || loggerClazz.getName().equals( call.getContext() );
    }

    private boolean matchedLevel( LogCall call )
    {
        return logLevel == null || logLevel == call.getLevel();
    }
}
