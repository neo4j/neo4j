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
package org.neo4j.logging;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IllegalFormatException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import org.neo4j.function.Consumer;
import org.neo4j.function.Function;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringEscapeUtils.escapeJava;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AssertableLogProvider extends AbstractLogProvider<Log> implements TestRule
{
    private final boolean debugEnabled;
    private final List<LogCall> logCalls = Collections.synchronizedList( new ArrayList<LogCall>() );

    public AssertableLogProvider()
    {
        this( false );
    }

    public AssertableLogProvider( boolean debugEnabled )
    {
        this.debugEnabled = debugEnabled;
    }

    @Override
    public Statement apply( final Statement base, org.junit.runner.Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                try
                {
                    base.evaluate();
                }
                catch ( Throwable failure )
                {
                    print( System.out );
                    throw failure;
                }
            }
        };
    }

    public void print( PrintStream out )
    {
        for ( LogCall call : logCalls )
        {
            out.println( call.toLogLikeString() );
            if ( call.throwable != null )
            {
                call.throwable.printStackTrace( out );
            }
        }
    }

    public enum Level
    {
        DEBUG,
        INFO,
        WARN,
        ERROR
    }

    private static final class LogCall
    {
        private final String context;
        private final Level level;
        private final String message;
        private final Object[] arguments;
        private final Throwable throwable;

        private LogCall( String context, Level level, String message, Object[] arguments, Throwable throwable )
        {
            this.level = level;
            this.context = context;
            this.message = message;
            this.arguments = arguments;
            this.throwable = throwable;
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder( "LogCall{" );
            builder.append( context );
            builder.append( " " );
            builder.append( level );
            builder.append( ", message=" );
            if ( message != null )
            {
                builder.append( '\'' ).append( escapeJava( message ) ).append( '\'' );
            } else
            {
                builder.append( "null" );
            }
            builder.append( ", arguments=" );
            if ( arguments != null )
            {
                builder.append( "[" );
                boolean first = true;
                for ( Object arg : arguments )
                {
                    if ( !first )
                    {
                        builder.append( ',' );
                    }
                    first = false;
                    builder.append( escapeJava( arg.toString() ) );
                }
                builder.append( "]" );
            } else
            {
                builder.append( "null" );
            }
            builder.append( ", throwable=" );
            if ( throwable != null )
            {
                builder.append( '\'' ).append( escapeJava( throwable.toString() ) ).append( '\'' );
            } else
            {
                builder.append( "null" );
            }
            builder.append( "}" );
            return builder.toString();
        }

        public String toLogLikeString()
        {
            String msg;
            if ( arguments != null )
            {
                try
                {
                    msg = format( message, arguments );
                }
                catch ( IllegalFormatException e )
                {
                    msg = format( "IllegalFormat{message: \"%s\", arguments: %s}",
                            message, Arrays.toString( arguments ) );
                }
            }
            else
            {
                msg = message;
            }
            return format( "%s @ %s: %s", level, context, msg );
        }
    }

    private class LogCallRecorder implements Logger
    {
        private final String context;
        private final Level level;

        public LogCallRecorder( String context, Level level )
        {
            this.context = context;
            this.level = level;
        }

        @Override
        public void log( String message )
        {
            logCalls.add( new LogCall( context, level, message, null, null ) );
        }

        @Override
        public void log( String message, Throwable throwable )
        {
            logCalls.add( new LogCall( context, level, message, null, throwable ) );
        }

        @Override
        public void log( String format, Object... arguments )
        {
            logCalls.add( new LogCall( context, level, format, arguments, null ) );
        }

        @Override
        public void bulk( Consumer<Logger> consumer )
        {
            consumer.accept( this );
        }
    }

    private class AssertableLog extends AbstractLog
    {
        private final Logger debugLogger;
        private final Logger infoLogger;
        private final Logger warnLogger;
        private final Logger errorLogger;

        AssertableLog( String context )
        {
            this.debugLogger = new LogCallRecorder( context, Level.DEBUG );
            this.infoLogger = new LogCallRecorder( context, Level.INFO );
            this.warnLogger = new LogCallRecorder( context, Level.WARN );
            this.errorLogger = new LogCallRecorder( context, Level.ERROR );
        }

        @Override
        public boolean isDebugEnabled()
        {
            return debugEnabled;
        }

        @Override
        public Logger debugLogger()
        {
            return debugLogger;
        }

        @Override
        public Logger infoLogger()
        {
            return infoLogger;
        }

        @Override
        public Logger warnLogger()
        {
            return warnLogger;
        }

        @Override
        public Logger errorLogger()
        {
            return errorLogger;
        }

        @Override
        public void bulk( Consumer<Log> consumer )
        {
            consumer.accept( this );
        }
    }

    @Override
    protected Log buildLog( Class loggingClass )
    {
        return new AssertableLog( loggingClass.getName() );
    }

    @Override
    protected Log buildLog( String context )
    {
        return new AssertableLog( context );
    }

    //
    // TEST TOOLS
    //

    private static final Matcher<Class> ANY_CLASS_MATCHER = any( Class.class );
    private static final Matcher<Level> DEBUG_LEVEL_MATCHER = equalTo( Level.DEBUG );
    private static final Matcher<Level> INFO_LEVEL_MATCHER = equalTo( Level.INFO );
    private static final Matcher<Level> WARN_LEVEL_MATCHER = equalTo( Level.WARN );
    private static final Matcher<Level> ERROR_LEVEL_MATCHER = equalTo( Level.ERROR );
    private static final Matcher<Level> ANY_LEVEL_MATCHER = any( Level.class );
    private static final Matcher<Object[]> NULL_ARGUMENTS_MATCHER = nullValue( Object[].class );
    private static final Matcher<Object[]> ANY_ARGUMENTS_MATCHER = any( Object[].class );
    private static final Matcher<Throwable> NULL_THROWABLE_MATCHER = nullValue( Throwable.class );
    private static final Matcher<Throwable> ANY_THROWABLE_MATCHER = any( Throwable.class );

    public static final class LogMatcher
    {
        private final Matcher<String> contextMatcher;
        private final Matcher<Level> levelMatcher;
        private final Matcher<String> messageMatcher;
        private final Matcher<? extends Object[]> argumentsMatcher;
        private final Matcher<? extends Throwable> throwableMatcher;

        public LogMatcher( Matcher<String> contextMatcher, Matcher<Level> levelMatcher, Matcher<String> messageMatcher, Matcher<? extends Object[]> argumentsMatcher, Matcher<? extends Throwable> throwableMatcher )
        {
            this.contextMatcher = contextMatcher;
            this.levelMatcher = levelMatcher;
            this.messageMatcher = messageMatcher;
            this.argumentsMatcher = argumentsMatcher;
            this.throwableMatcher = throwableMatcher;
        }

        protected boolean matches( LogCall logCall )
        {
            return logCall != null &&
                    contextMatcher.matches( logCall.context ) &&
                    levelMatcher.matches( logCall.level ) &&
                    messageMatcher.matches( logCall.message ) &&
                    argumentsMatcher.matches( logCall.arguments ) &&
                    throwableMatcher.matches( logCall.throwable );
        }

        @Override
        public String toString()
        {
            Description description = new StringDescription();
            description.appendText( "LogMatcher{" );
            description.appendDescriptionOf( contextMatcher );
            description.appendText( ", " );
            description.appendDescriptionOf( levelMatcher );
            description.appendText( ", message=" );
            description.appendDescriptionOf( messageMatcher );
            description.appendText( ", arguments=" );
            description.appendDescriptionOf( argumentsMatcher );
            description.appendText( ", throwable=" );
            description.appendDescriptionOf( throwableMatcher );
            description.appendText( "}" );
            return description.toString();
        }
    }

    public static final class LogMatcherBuilder
    {
        private final Matcher<String> contextMatcher;

        private LogMatcherBuilder( Matcher<String> contextMatcher )
        {
            this.contextMatcher = contextMatcher;
        }

        public LogMatcher debug( String message )
        {
            return new LogMatcher( contextMatcher, DEBUG_LEVEL_MATCHER, equalTo( message ), NULL_ARGUMENTS_MATCHER, NULL_THROWABLE_MATCHER );
        }

        public LogMatcher debug( Matcher<String> messageMatcher )
        {
            return new LogMatcher( contextMatcher, DEBUG_LEVEL_MATCHER, messageMatcher, NULL_ARGUMENTS_MATCHER, NULL_THROWABLE_MATCHER );
        }

        public LogMatcher debug( Matcher<String> messageMatcher, Matcher<Throwable> throwableMatcher )
        {
            return new LogMatcher( contextMatcher, DEBUG_LEVEL_MATCHER, messageMatcher, NULL_ARGUMENTS_MATCHER, throwableMatcher );
        }

        public LogMatcher debug( String format, Object... arguments )
        {
            return debug( equalTo( format ), arguments );
        }

        public LogMatcher debug( Matcher<String> format, Object... arguments )
        {
            return new LogMatcher( contextMatcher, DEBUG_LEVEL_MATCHER, format, arrayContaining( ensureMatchers( arguments ) ), NULL_THROWABLE_MATCHER );
        }

        public LogMatcher info( String message )
        {
            return new LogMatcher( contextMatcher, INFO_LEVEL_MATCHER, equalTo( message ), NULL_ARGUMENTS_MATCHER, NULL_THROWABLE_MATCHER );
        }

        public LogMatcher info( Matcher<String> messageMatcher )
        {
            return new LogMatcher( contextMatcher, INFO_LEVEL_MATCHER, messageMatcher, NULL_ARGUMENTS_MATCHER, NULL_THROWABLE_MATCHER );
        }

        public LogMatcher info( Matcher<String> messageMatcher, Matcher<Throwable> throwableMatcher )
        {
            return new LogMatcher( contextMatcher, INFO_LEVEL_MATCHER, messageMatcher, NULL_ARGUMENTS_MATCHER, throwableMatcher );
        }

        public LogMatcher info( String format, Object... arguments )
        {
            return info( equalTo( format ), arguments );
        }

        public LogMatcher info( Matcher<String> format, Object... arguments )
        {
            return new LogMatcher( contextMatcher, INFO_LEVEL_MATCHER, format, arrayContaining( ensureMatchers( arguments ) ), NULL_THROWABLE_MATCHER );
        }

        public LogMatcher warn( String message )
        {
            return new LogMatcher( contextMatcher, WARN_LEVEL_MATCHER, equalTo( message ), NULL_ARGUMENTS_MATCHER, NULL_THROWABLE_MATCHER );
        }

        public LogMatcher warn( Matcher<String> messageMatcher )
        {
            return new LogMatcher( contextMatcher, WARN_LEVEL_MATCHER, messageMatcher, NULL_ARGUMENTS_MATCHER, NULL_THROWABLE_MATCHER );
        }

        public LogMatcher warn( Matcher<String> messageMatcher, Matcher<Throwable> throwableMatcher )
        {
            return new LogMatcher( contextMatcher, WARN_LEVEL_MATCHER, messageMatcher, NULL_ARGUMENTS_MATCHER, throwableMatcher );
        }

        public LogMatcher warn( String format, Object... arguments )
        {
            return warn( equalTo( format ), arguments );
        }

        public LogMatcher warn( Matcher<String> format, Object... arguments )
        {
            return new LogMatcher( contextMatcher, WARN_LEVEL_MATCHER, format, arrayContaining( ensureMatchers( arguments ) ), NULL_THROWABLE_MATCHER );
        }

        public LogMatcher error( String message )
        {
            return new LogMatcher( contextMatcher, ERROR_LEVEL_MATCHER, equalTo( message ), NULL_ARGUMENTS_MATCHER, NULL_THROWABLE_MATCHER );
        }

        public LogMatcher error( Matcher<String> messageMatcher )
        {
            return new LogMatcher( contextMatcher, ERROR_LEVEL_MATCHER, messageMatcher, NULL_ARGUMENTS_MATCHER, NULL_THROWABLE_MATCHER );
        }

        public LogMatcher error( Matcher<String> messageMatcher, Matcher<? extends Throwable> throwableMatcher )
        {
            return new LogMatcher( contextMatcher, ERROR_LEVEL_MATCHER, messageMatcher, NULL_ARGUMENTS_MATCHER, throwableMatcher );
        }

        public LogMatcher error( String format, Object... arguments )
        {
            return error( equalTo( format ), arguments );
        }

        public LogMatcher error( Matcher<String> format, Object... arguments )
        {
            return new LogMatcher( contextMatcher, ERROR_LEVEL_MATCHER, format, arrayContaining( ensureMatchers( arguments ) ), NULL_THROWABLE_MATCHER );
        }

        @SuppressWarnings( "unchecked" )
        private Matcher<Object>[] ensureMatchers( Object... arguments )
        {
            List<Matcher> matchers = new ArrayList<>();
            for ( Object arg : arguments )
            {
                if ( arg instanceof Matcher )
                {
                    matchers.add( (Matcher) arg );
                } else {
                    matchers.add( equalTo( arg ) );
                }
            }
            return matchers.toArray( new Matcher[arguments.length] );
        }
    }

    public static LogMatcherBuilder inLog( Class logClass )
    {
        return inLog( equalTo( logClass.getName() ) );
    }

    public static LogMatcherBuilder inLog( String context )
    {
        return inLog( equalTo( context ) );
    }

    public static LogMatcherBuilder inLog( Matcher<String> contextMatcher )
    {
        return new LogMatcherBuilder( contextMatcher );
    }

    public void assertExactly( LogMatcher... expected )
    {
        Iterator<LogMatcher> expectedIterator = asList( expected ).iterator();

        synchronized (logCalls)
        {
            Iterator<LogCall> callsIterator = logCalls.iterator();

            while ( expectedIterator.hasNext() )
            {
                if ( callsIterator.hasNext() )
                {
                    LogMatcher logMatcher = expectedIterator.next();
                    LogCall logCall = callsIterator.next();
                    if ( !logMatcher.matches( logCall ) )
                    {
                        fail( format( "Log call did not match expectation\n  Expected: %s\n  Call was: %s", logMatcher, logCall ) );
                    }
                } else
                {
                    fail( format( "Got fewer log calls than expected. The missing log calls were:\n%s", describe( expectedIterator ) ) );
                }
            }

            if ( callsIterator.hasNext() )
            {
                fail( format( "Got more log calls than expected. The remaining log calls were:\n%s", serialize( callsIterator ) ) );
            }
        }
    }

    @SafeVarargs
    public final void assertContainsLogCallsMatching( int logSkipCount, Matcher<String>... matchers )
    {
        synchronized ( logCalls )
        {
            assertEquals( logCalls.size(), logSkipCount + matchers.length );
            for ( int i = 0; i < matchers.length; i++ )
            {
                LogCall logCall = logCalls.get( logSkipCount + i );
                Matcher<String> matcher = matchers[i];

                if ( !matcher.matches( logCall.message ) )
                {
                    StringDescription description = new StringDescription();
                    description.appendDescriptionOf( matcher );
                    fail( format( "Expected log statement with message as %s, but none found. Actual log call was:\n%s",
                            description.toString(), logCall.toString() ) );
                }
            }
        }
    }

    public void assertContainsThrowablesMatching( int logSkipCount,  Throwable... throwables )
    {
        synchronized ( logCalls )
        {
            assertEquals( logCalls.size(), logSkipCount + throwables.length );
            for ( int i = 0; i < throwables.length; i++ )
            {
                LogCall logCall = logCalls.get( logSkipCount + i );
                Throwable throwable = throwables[i];

                if ( logCall.throwable == null && throwable != null ||
                        logCall.throwable != null && logCall.throwable.getClass() != throwable.getClass() )
                {
                    fail( format( "Expected %s, but was:\n%s",
                            throwable, logCall.throwable ) );
                }
            }
        }
    }

    /**
     * Note: Does not care about ordering.
     */
    public void assertAtLeastOnce( LogMatcher... expected )
    {
        Set<LogMatcher> expectedMatchers = new HashSet<>( Arrays.asList( expected ) );
        synchronized (logCalls)
        {
            for ( LogCall logCall : logCalls )
            {
                for ( LogMatcher matcher : expectedMatchers )
                {
                    if ( matcher.matches( logCall ) )
                    {
                        expectedMatchers.remove( matcher );
                        break;
                    }
                }
            }

            if ( expectedMatchers.size() > 0 )
            {
                fail( format(
                        "These log calls were expected, but never occurred:\n%s\nActual log calls were:\n%s",
                        describe( expectedMatchers.iterator() ),
                        serialize( logCalls.iterator() )
                ) );
            }
        }
    }

    public void assertNone( LogMatcher notExpected )
    {
        synchronized (logCalls)
        {
            for ( LogCall logCall : logCalls )
            {
                if ( notExpected.matches( logCall ) )
                {
                    fail( format(
                            "Log call was not expected, but occurred:\n%s\n", logCall.toString()
                    ) );
                }
            }
        }
    }

    public void assertContainsLogCallContaining( String partOfMessage )
    {
        synchronized (logCalls)
        {
            for ( LogCall logCall : logCalls )
            {
                if ( logCall.toString().contains( partOfMessage ) )
                {
                    return;
                }
            }
            fail( format(
                    "Expected at least one log statement containing '%s', but none found. Actual log calls were:\n%s", partOfMessage, serialize( logCalls.iterator() )
            ) );
        }
    }

    public void assertContainsMessageContaining( String partOfMessage )
    {
        synchronized (logCalls)
        {
            for ( LogCall logCall : logCalls )
            {
                if ( logCall.message.contains( partOfMessage ) )
                {
                    return;
                }
            }
            fail( format(
                    "Expected at least one log statement containing '%s', but none found. Actual log calls were:\n%s", partOfMessage, serialize( logCalls.iterator() )
            ) );
        }
    }

    public void assertContainsMessageMatching( Matcher<String> messageMatcher )
    {
        synchronized (logCalls)
        {
            for ( LogCall logCall : logCalls )
            {
                if ( messageMatcher.matches( logCall.message ) )
                {
                    return;
                }
            }
            StringDescription description = new StringDescription();
            description.appendDescriptionOf( messageMatcher );
            fail( format(
                    "Expected at least one log statement with message as %s, but none found. Actual log calls were:\n%s", description.toString(), serialize( logCalls.iterator() )
            ) );
        }
    }

    public void assertNoLoggingOccurred()
    {
        if ( logCalls.size() != 0 )
        {
            fail( format( "Expected no log messages at all, but got:\n%s", serialize( logCalls.iterator() ) ) );
        }
    }

    /**
     * Clear this logger for re-use.
     */
    public void clear()
    {
        logCalls.clear();
    }

    public String serialize()
    {
        return serialize( logCalls.iterator(), new Function<LogCall,String>()
        {
            @Override
            public String apply( LogCall call )
            {
                return call.toLogLikeString();
            }
        } );
    }

    private String describe( Iterator<LogMatcher> matchers )
    {
        StringBuilder sb = new StringBuilder();
        while ( matchers.hasNext() )
        {
            sb.append( matchers.next().toString() );
            sb.append( "\n" );
        }
        return sb.toString();
    }

    private String serialize( Iterator<LogCall> events )
    {
        return serialize( events, new Function<LogCall,String>()
        {
            @Override
            public String apply( LogCall call )
            {
                return call.toString();
            }
        } );
    }

    private String serialize( Iterator<LogCall> events, Function<LogCall,String> serializer )
    {
        StringBuilder sb = new StringBuilder();
        while ( events.hasNext() )
        {
            sb.append( serializer.apply( events.next() ) );
            sb.append( "\n" );
        }
        return sb.toString();
    }
}
