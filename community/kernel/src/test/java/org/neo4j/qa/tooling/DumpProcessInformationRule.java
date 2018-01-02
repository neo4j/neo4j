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
package org.neo4j.qa.tooling;

import java.io.File;
import java.io.PrintStream;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.hamcrest.Matcher;
import org.junit.rules.ExternalResource;

import org.neo4j.logging.NullLogProvider;

public class DumpProcessInformationRule extends ExternalResource
{
    public interface Dump
    {
        void dump() throws Exception;
    }
    
    public static Dump localVm( final PrintStream out )
    {
        return new Dump()
        {
            @Override
            public void dump()
            {
                DumpVmInformation.dumpVmInfo( out );
            }
        };
    }
    
    public static Dump otherVm( final Matcher<String> processFilter, final File baseDir )
    {
        return new Dump()
        {
            @Override
            public void dump() throws Exception
            {
                new DumpProcessInformation( NullLogProvider.getInstance(), baseDir ).doThreadDump( processFilter );
            }
        };
    }
    
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool( 2 );
    private final long duration;
    private final TimeUnit timeUnit;
    private volatile ScheduledFuture<?> thunk = null;
    private final Dump[] dumps;

    /**
     * Dumps process information about processes on the local machine, filtered by processFilter
     */
    public DumpProcessInformationRule( long duration, TimeUnit timeUnit, Dump... dumps )
    {
        this.duration = duration;
        this.timeUnit = timeUnit;
        this.dumps = dumps;
    }
    
    @Override
    protected synchronized void before() throws Throwable
    {
        if ( null == thunk )
        {
            super.before();
            thunk = executor.schedule( new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    dump();
                    return null;
                }
            }, duration, timeUnit );
        }
        else
        {
            throw new IllegalStateException( "process dumping thunk already started" );
        }
    }

    @Override
    protected synchronized void after()
    {
        if ( null != thunk && !thunk.isDone() )
        {
            thunk.cancel( true );
        }
        thunk = null;
        super.after();
    }

    public void dump() throws Exception
    {
        for ( Dump dump : dumps )
        {
            dump.dump();
        }
    }
}
