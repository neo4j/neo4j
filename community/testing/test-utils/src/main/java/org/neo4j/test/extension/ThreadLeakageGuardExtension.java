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
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.util.ExceptionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Arrays.stream;

public class ThreadLeakageGuardExtension implements AfterAllCallback, BeforeAllCallback
{
    private static final long MAXIMUM_WAIT_TIME_MILLIS = 90_000;
    private static final String KEY = "ThreadLeakageExtension";
    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create( KEY );
    private final StacktraceHolderException stacktraceHolderException = new StacktraceHolderException();
    private static final List<String> THREAD_NAME_FILTER = Arrays.asList(
            "ForkJoinPool",
            "Cleaner",
            "PageCacheRule",                    //Ignoring page cache
            "MuninnPageCache",                  //Ignoring page cache
            "Attach Listener",                  //IDE thread
            "process reaper",                   //Unix system thread
            "neo4j.BoltNetworkIO",              //Bolt threads use non-blocking exit
            "globalEventExecutor",              //related to Bolt threads
            "HttpClient",                       //same as bolt, non-blocking exit
            "Keep-Alive-Timer",                 //JVM thread for http-connections
            "ObjectCleanerThread",              //Netty (version < 4.1.28) thread from java driver
            "neo4j.StorageMaintenance",         //Sometimes erroneously leaked on database shutdown, possible race-condition, to be fixed!
            "neo4j.TransactionTimeoutMonitor",  //Sometimes erroneously leaked on database shutdown, possible race-condition, to be fixed!
            "neo4j.CheckPoint",                 //Sometimes erroneously leaked on database shutdown, possible race-condition, to be fixed!
            "neo4j.IndexSampling",              //Sometimes erroneously leaked on database shutdown, possible race-condition, to be fixed!
            "neo4j.ThroughputMonitor",          //Issue with thread leak guard and scheduling of recurring jobs as above?
            "junit-jupiter-timeout-watcher"
    );

    @Override
    public void afterAll( ExtensionContext context ) throws Exception
    {
        if ( skipThreadLeakageGuard( context ) )
        {
            return;
        }

        List<String> leakedThreads = new ArrayList<>();

        final ThreadNamesCollection startupThreads = getStore( context ).remove( KEY, ThreadNamesCollection.class );
        long startTime = System.currentTimeMillis();
        for ( Thread thread : getActiveThreads() )
        {
            if ( !startupThreads.contains( getThreadID( thread ) ) )
            {
                long waitTimeForThread = MAXIMUM_WAIT_TIME_MILLIS - ( System.currentTimeMillis() - startTime );
                if ( thread.isAlive() && waitTimeForThread > 0 )
                {
                    thread.join( waitTimeForThread );
                }

                if ( thread.isAlive() )
                {
                    leakedThreads.add( format( "%s (ID:%d, Group:%s)\n%s\n",
                            thread.getName(),
                            thread.getId(),
                            getThreadGroupName( thread ),
                            StacktraceToString( thread.getStackTrace() ) ) );
                }
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
        if ( skipThreadLeakageGuard( context ) )
        {
            return;
        }

        Set<String> startupThreads = getActiveThreads().stream()
                .map( ThreadLeakageGuardExtension::getThreadID )
                .collect( Collectors.toCollection( ThreadNamesCollection::new ));

        getStore( context ).put( KEY, startupThreads );
    }

    private static boolean skipThreadLeakageGuard( ExtensionContext context )
    {
        return AnnotationSupport.isAnnotated( context.getRequiredTestClass(), SkipThreadLeakageGuard.class ) || isConcurrentExecution();
    }

    private static boolean isConcurrentExecution()
    {
        return "concurrent".equals( System.getProperty( "junit.jupiter.execution.parallel.mode.classes.default" ) );
    }

    private static Collection<Thread> getActiveThreads()
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

        return stream( threads )
                .filter( Objects::nonNull )
                .filter( Thread::isAlive )
                .filter( thread -> THREAD_NAME_FILTER.stream().noneMatch( prefix -> thread.getName().startsWith( prefix ) ) )
                .collect( Collectors.toSet() );
    }

    private static String getThreadID( Thread thread )
    {
        return format( "%s-%d-%s", thread.getName(), thread.getId(), getThreadGroupName( thread ) );
    }

    private static String getThreadGroupName( Thread thread )
    {
        ThreadGroup group = thread.getThreadGroup();
        return group != null ? group.getName() : "unknown group";

    }

    private static ExtensionContext.Store getStore( ExtensionContext context )
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

    private static class ThreadNamesCollection extends HashSet<String>
    {

    }
}

