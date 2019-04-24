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
package org.neo4j.test.extension;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContextException;
import org.junit.platform.commons.util.ExceptionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;

public class ThreadLeakageGuardExtension implements AfterAllCallback, BeforeAllCallback
{
    private static final String KEY = "ThreadLeakageExtension";
    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create( KEY );
    private final StacktraceHolderException stacktraceHolderException = new StacktraceHolderException();

    @Override
    public void afterAll( ExtensionContext context ) throws Exception
    {
        List<String> leakedThreads = new ArrayList<>();

        @SuppressWarnings( {"unchecked"} )
        final Set<String> startupThreads = getStore( context ).remove( KEY, Set.class );
        for ( Thread thread : getActiveThreads() )
        {
            if ( !startupThreads.contains( getThreadID( thread ) ) )
            {
                leakedThreads.add( format( "%s (ID:%d, Group:%s)\n%s\n",
                        thread.getName(),
                        thread.getId(),
                        thread.getThreadGroup().getName(),
                        StacktraceToString( thread.getStackTrace() ) ) );
            }
        }

        if ( !leakedThreads.isEmpty() )
        {
            throw new ExtensionContextException( format( "%d leaked thread(s) detected:\n%s",
                    leakedThreads.size(),
                    leakedThreads.toString() ) );
        }
    }

    @Override
    public void beforeAll( ExtensionContext context ) throws Exception
    {
        Set<String> startupThreads = new HashSet<>();
        getActiveThreads().forEach( ( Thread thread ) ->
        {
            startupThreads.add( getThreadID( thread ) );
        } );
        getStore( context ).put( KEY, startupThreads );
    }

    private Collection<Thread> getActiveThreads()
    {
        ThreadGroup root = Thread.currentThread().getThreadGroup();
        while ( root.getParent() != null )
        {
            root = root.getParent();
        }

        Thread[] threads;
        int numThreads = Thread.activeCount() + 1;
        do
        {
            threads = new Thread[numThreads * 2];
            numThreads = root.enumerate( threads, true );
        }
        while ( numThreads >= threads.length );

        Set<Thread> threadCollection = new HashSet<>();
        Collections.addAll( threadCollection, threads );
        threadCollection.remove( null );

        List<String> filter = Arrays.asList( "ForkJoinPool", "Cleaner", "PageCacheRule", "MuninnPageCache" );
        filter.forEach( ( String prefix ) ->
        {
            threadCollection.removeIf( ( Thread t ) -> t.getName().startsWith( prefix ) );
        } );
        return threadCollection;
    }

    private String getThreadID( Thread thread )
    {
        return format( "%s-%d-%s", thread.getName(), thread.getId(), thread.getThreadGroup().getName() );
    }

    private ExtensionContext.Store getStore( ExtensionContext context )
    {
        return context.getStore( NAMESPACE );
    }

    private String StacktraceToString( StackTraceElement[] stackTraceElements )
    {
        stacktraceHolderException.setStackTrace( stackTraceElements );
        return ExceptionUtils.readStackTrace( stacktraceHolderException );
    }

    private static class StacktraceHolderException extends RuntimeException
    {

        @Override
        public synchronized Throwable fillInStackTrace()
        {
            return this;
        }

        @Override
        public String toString()
        {
            return "";
        }
    }
}

