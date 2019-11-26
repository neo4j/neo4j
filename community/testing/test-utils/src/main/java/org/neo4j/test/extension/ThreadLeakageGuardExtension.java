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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.SuppressOutput.Voice;

import static java.lang.String.format;
import static java.lang.management.ManagementFactory.getRuntimeMXBean;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toCollection;
import static org.neo4j.test.extension.SuppressOutputExtension.SUPPRESS_OUTPUT_NAMESPACE;
import static org.neo4j.util.FeatureToggles.flag;

public class ThreadLeakageGuardExtension implements AfterAllCallback, BeforeAllCallback
{
    private static final boolean PRINT_ONLY = flag( ThreadLeakageGuardExtension.class, "PRINT_ONLY", false );

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
            "junit-jupiter-timeout-watcher"
    );

    @Override
    public void afterAll( ExtensionContext context ) throws Exception
    {
        if ( skipThreadLeakageGuard( context ) )
        {
            return;
        }

        ThreadIds beforeThreads = getStore( context ).remove( KEY, ThreadIds.class );

        List<String> leakedThreads = new ArrayList<>();
        long startTime = System.currentTimeMillis();

        for ( Thread afterThread : getActiveThreads() )
        {
            if ( beforeThreads.contains( afterThread.getId() ) )
            {
                continue;
            }

            long remainingWait = MAXIMUM_WAIT_TIME_MILLIS - ( System.currentTimeMillis() - startTime );
            if ( afterThread.isAlive() && remainingWait > 0 )
            {
                afterThread.join( remainingWait );
            }

            if ( afterThread.isAlive() )
            {
                leakedThreads.add( describeThread( afterThread ) );
            }
        }

        beforeThreads.clear();

        if ( !leakedThreads.isEmpty() )
        {
            String message = format( "%d leaked thread(s) detected:\n%s", leakedThreads.size(), leakedThreads.toString() );
            if ( PRINT_ONLY )
            {
                printError( context, message );
            }
            else
            {
                throw new ExtensionContextException( message );
            }
        }
    }

    private void printError( ExtensionContext context, String message )
    {
        ExtensionContext.Store store = context.getStore( SUPPRESS_OUTPUT_NAMESPACE );
        SuppressOutput suppressOutput = store.get( SuppressOutputExtension.SUPPRESS_OUTPUT, SuppressOutput.class );

        PrintStream errorStream = getErrorStream( suppressOutput );
        errorStream.println( message );
    }

    private PrintStream getErrorStream( SuppressOutput suppressOutput )
    {
        PrintStream errStream = System.err;
        if ( suppressOutput == null )
        {
            return errStream;
        }

        Voice errorVoice = suppressOutput.getErrorVoice();
        if ( errorVoice == null )
        {
            return errStream;
        }

        Optional<PrintStream> originalStream = errorVoice.originalStream();

        if ( originalStream.isPresent() )
        {
            errStream = originalStream.get();
        }

        return errStream;
    }

    @Override
    public void beforeAll( ExtensionContext context )
    {
        if ( skipThreadLeakageGuard( context ) )
        {
            return;
        }

        ThreadIds activeThreads = getActiveThreads()
                .stream()
                .map( Thread::getId )
                .collect( toCollection( ThreadIds::new ) );

        getStore( context ).put( KEY, activeThreads );
    }

    private static boolean skipThreadLeakageGuard( ExtensionContext context )
    {
        return AnnotationSupport.isAnnotated( context.getRequiredTestClass(), SkipThreadLeakageGuard.class ) || isConcurrentExecution();
    }

    private static boolean isConcurrentExecution()
    {
        return "concurrent".equals( System.getProperty( "junit.jupiter.execution.parallel.mode.classes.default" ) );
    }

    private static Set<Thread> getActiveThreads()
    {
        ThreadGroup root = Thread.currentThread().getThreadGroup();
        while ( root.getParent() != null )
        {
            root = root.getParent();
        }

        Thread[] threads;
        int numThreads = root.activeCount() + 1;
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

    private String describeThread( Thread thread )
    {
        String basicInfo = format( "%s %s (PID:%s, TID:%d, Groups:%s)",
                thread.getName(),
                thread.getState(),
                getRuntimeMXBean().getName(),
                thread.getId(),
                describeThreadGroupChain( thread ) );

        return format( "%s%n%s%n", basicInfo, describeStack( thread ) );
    }

    private static ExtensionContext.Store getStore( ExtensionContext context )
    {
        return context.getStore( NAMESPACE );
    }

    private String describeStack( Thread thread )
    {
        StackTraceElement[] stackTraceElements = thread.getStackTrace();
        stacktraceHolderException.setStackTrace( stackTraceElements );
        return ExceptionUtils.readStackTrace( stacktraceHolderException );
    }

    private static String describeThreadGroupChain( Thread thread )
    {
        ThreadGroup group = thread.getThreadGroup();
        if ( group == null )
        {
            return "<dead>";
        }

        StringBuilder str = new StringBuilder( group.getName() );

        while ( (group = group.getParent()) != null )
        {
            str.append( ":" ).append( group.getName() );
        }

        return str.toString();
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

    private static class ThreadIds extends HashSet<Long>
    {
    }
}
