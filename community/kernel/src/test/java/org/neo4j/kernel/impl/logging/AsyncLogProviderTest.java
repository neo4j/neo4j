/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.logging;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.function.Function;
import org.neo4j.function.Predicate;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.logging.BufferingLog;
import org.neo4j.logging.BufferingLog.LogMessage;
import org.neo4j.logging.BufferingLog.LogVisitor;
import org.neo4j.logging.BufferingLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.kernel.impl.logging.AsyncEventLogging.DEFAULT_ASYNC_ERROR_HANDLER;

@RunWith( Parameterized.class )
public class AsyncLogProviderTest
{
    @Parameters( name = "{0}" )
    public static Collection<Object[]> data()
    {
        Collection<Object[]> data = new ArrayList<>();
        data.add( new Object[] {Level.DEBUG, new Function<Log,Logger>()
        {
            @Override
            public Logger apply( Log log )
            {
                return log.debugLogger();
            }
        }} );
        data.add( new Object[] {Level.INFO, new Function<Log,Logger>()
        {
            @Override
            public Logger apply( Log log )
            {
                return log.infoLogger();
            }
        }} );
        data.add( new Object[] {Level.WARN, new Function<Log,Logger>()
        {
            @Override
            public Logger apply( Log log )
            {
                return log.warnLogger();
            }
        }} );
        data.add( new Object[] {Level.ERROR, new Function<Log,Logger>()
        {
            @Override
            public Logger apply( Log log )
            {
                return log.errorLogger();
            }
        }} );
        return data;
    }

    @Parameter( 0 )
    public Level level;

    @Parameter( 1 )
    public Function<Log,Logger> loggerFunction;

    @Rule
    public final LifeRule life = new LifeRule( true );

    @Test
    public void shouldProcessLogEventsSeparately() throws Exception
    {
        // GIVEN
        BufferingLogProvider actual = new BufferingLogProvider();
        AsyncLogProvider asyncLogProvider = new AsyncLogProvider( actual, life.add(
                new AsyncEventLogging( DEFAULT_ASYNC_ERROR_HANDLER, newSingleThreadExecutor() ) ) );
        Log log = asyncLogProvider.getLog( AsyncLogProviderTest.class );
        Logger logger = logger( log, level );

        logger.log( "First message" );
        logger.log( "Second %s", "message" );
        logger.log( "", new RuntimeException( "Third message" ) );
        BufferingLog actualLog = actual.getLog( AsyncLogProviderTest.class );
        assertEquals( 0, actualLog.toString().length() );

        // THEN
        assertLogContains( actualLog,
                "First message",
                "Second message",
                "Third message",
                RuntimeException.class.getSimpleName() );
    }

    private Logger logger( Log log, Level level )
    {
        switch ( level )
        {
        case DEBUG: return log.debugLogger();
        case INFO: return log.infoLogger();
        case WARN: return log.warnLogger();
        case ERROR: return log.errorLogger();
        default: throw new IllegalArgumentException( level.name() );
        }
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldContinueToLogAfterOomDuringLogStatement() throws Exception
    {
        // GIVEN
        ThrowingLogProvider actual = new ThrowingLogProvider();
        Listener<Throwable> errorHandler = mock( Listener.class );
        AsyncLogProvider asyncLogProvider = new AsyncLogProvider( actual, life.add(
                new AsyncEventLogging( errorHandler, newSingleThreadExecutor() ) ) );

        // Make sure that the setup in this test is working as it should
        Log log = asyncLogProvider.getLog( getClass() );
        log.info( "Testing" );
        waitForLogMessage( actual, "Testing" );

        // WHEN making the actual log provider, i.e. the processing of the log event throw some sort of error
        actual.makeNextLogStatementThrow();
        log.info( "Should throw" );

        // THEN
        log.info( "After error" );
        waitForLogMessage( actual, "After error" );
        assertFalse( actual.hasLogged( "Should throw" ) );
        verify( errorHandler, times( 1 ) ).receive( any( Throwable.class ) );
    }

    private static void await( Predicate<?> condition ) throws InterruptedException
    {
        long endTime = currentTimeMillis() + SECONDS.toMillis( 30 );
        while ( !condition.test( null ) )
        {
            Thread.sleep( 10 );
            if ( currentTimeMillis() > endTime )
            {
                fail( condition + " not fullfilled" );
            }
        }
    }

    private void waitForLogMessage( final ThrowingLogProvider actualLogProvider, final String message )
            throws InterruptedException
    {
        await( new Predicate<Void>()
        {
            @Override
            public boolean test( Void t )
            {
                return actualLogProvider.hasLogged( message );
            }
        } );
    }

    private void waitForLogMessage( final BufferingLog log, final String message ) throws InterruptedException
    {
        await( new Predicate<Void>()
        {
            @Override
            public boolean test( Void t )
            {
                return log.accept( presenceOfLogStatement( message ) );
            }
        } );
    }

    private void assertLogContains( BufferingLog log, String... messages ) throws InterruptedException
    {
        for ( String message : messages )
        {
            waitForLogMessage( log, message );
        }
    }

    private LogVisitor presenceOfLogStatement( final String statement )
    {
        return new LogVisitor()
        {
            @Override
            public boolean visit( LogMessage message )
            {
                return message.level().equals( level ) && message.toString().contains( statement );
            }
        };
    }
}
