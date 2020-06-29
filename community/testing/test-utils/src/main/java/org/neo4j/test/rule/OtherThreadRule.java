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
package org.neo4j.test.rule;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.test.OtherThreadExecutor;

import static java.util.concurrent.TimeUnit.SECONDS;

public class OtherThreadRule implements TestRule
{
    private String name;
    private long timeout;
    private TimeUnit unit;
    private volatile OtherThreadExecutor executor;

    public OtherThreadRule()
    {
        this( null );
    }

    public OtherThreadRule( String name )
    {
        set( name, 60, SECONDS );
    }

    public OtherThreadRule( long timeout, TimeUnit unit )
    {
        set( null, timeout, unit );
    }

    public void set( long timeout, TimeUnit unit )
    {
        this.timeout = timeout;
        this.unit = unit;
    }

    public void set( String name, long timeout, TimeUnit unit )
    {
        this.name = name;
        this.timeout = timeout;
        this.unit = unit;
    }

    public <RESULT> Future<RESULT> execute( Callable<RESULT> cmd )
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

    public OtherThreadExecutor get()
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
        OtherThreadExecutor otherThread = executor;
        if ( otherThread == null )
        {
            return "OtherThreadRule[state=dead]";
        }
        return otherThread.toString();
    }

    // Implementation of life cycles

    public void beforeEach( ExtensionContext context )
    {
        String displayName = context.getDisplayName();

        String threadName = name != null
                ? name + '-' + displayName
                : displayName;
        init( threadName );
    }

    public void afterEach()
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
                init( threadName );
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

    public void init( String threadName )
    {
        executor = new OtherThreadExecutor( threadName, timeout, unit );
    }

    public void close()
    {
        executor.close();
    }
}
