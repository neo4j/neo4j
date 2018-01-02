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
package org.neo4j.test;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static java.util.concurrent.TimeUnit.SECONDS;

public class OtherThreadRule<STATE> implements TestRule
{
    private final String name;
    private final long timeout;
    private final TimeUnit unit;
    private volatile OtherThreadExecutor<STATE> executor;

    public OtherThreadRule()
    {
        this( null );
    }

    public OtherThreadRule( String name )
    {
        this( name, 10, SECONDS );
    }

    public OtherThreadRule( long timeout, TimeUnit unit )
    {
        this( null, timeout, unit );
    }

    public OtherThreadRule( String name, long timeout, TimeUnit unit )
    {
        this.name = name;
        this.timeout = timeout;
        this.unit = unit;
    }

    public <RESULT> Future<RESULT> execute( OtherThreadExecutor.WorkerCommand<STATE, RESULT> cmd )
    {
        Future<RESULT> future = executor.executeDontWait( cmd );
        try
        {
            executor.awaitStartExecuting();
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( "Interrupted while awaiting start of execution.", e );
        }
        return future;
    }

    protected STATE initialState()
    {
        return null;
    }

    public static Matcher<OtherThreadRule> isWaiting()
    {
        return isThreadState( Thread.State.WAITING, Thread.State.TIMED_WAITING );
    }

    public static Matcher<OtherThreadRule> isThreadState( final Thread.State... eitherOfStates )
    {
        return new TypeSafeMatcher<OtherThreadRule>()
        {
            @Override
            protected boolean matchesSafely( OtherThreadRule rule )
            {
                try
                {
                    rule.executor.waitUntilThreadState( eitherOfStates );
                    return true;
                }
                catch ( TimeoutException e )
                {
                    rule.executor.printStackTrace( System.err );
                    return false;
                }
            }

            @Override
            public void describeTo( org.hamcrest.Description description )
            {
                description.appendText( "Thread blocked in state WAITING" );
            }
        };
    }

    public OtherThreadExecutor<STATE> get()
    {
        return executor;
    }

    public void interrupt()
    {
        executor.interrupt();
    }

    @Override
    public String toString()
    {
        OtherThreadExecutor<STATE> otherThread = executor;
        if ( otherThread == null )
        {
            return "OtherThreadRule[state=dead]";
        }
        return otherThread.toString();
    }

    // Implementation of TestRule

    @Override
    public Statement apply( final Statement base, final Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                String threadName = name != null
                        ? name + "-" + description.getDisplayName()
                        : description.getDisplayName();
                executor = new OtherThreadExecutor<>( threadName, timeout, unit, initialState() );
                try
                {
                    base.evaluate();
                }
                finally
                {
                    try
                    {
                        executor.close();
                    }
                    finally
                    {
                        executor = null;
                    }
                }
            }
        };
    }
}
