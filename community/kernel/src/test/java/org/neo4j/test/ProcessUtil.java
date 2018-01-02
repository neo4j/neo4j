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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.helpers.FutureAdapter;

import static java.util.Arrays.asList;

public class ProcessUtil
{
    public static void executeSubProcess( Class<?> mainClass, long timeout, TimeUnit unit,
            String... arguments ) throws Exception
    {
        Future<Integer> future = startSubProcess( mainClass, arguments );
        int result = future.get( timeout, unit );
        if ( result != 0 )
        {
            throw new RuntimeException( "Process for " + mainClass +
                    " with arguments " + Arrays.toString( arguments ) +
                    " failed, returned exit value " + result );
        }
    }
    
    public static Future<Integer> startSubProcess( Class<?> mainClass, String... arguments ) throws IOException
    {
        List<String> args = new ArrayList<>();
        args.addAll( asList( "java", "-cp", System.getProperty( "java.class.path" ), mainClass.getName() ) );
        args.addAll( asList( arguments ) );
        Process process = Runtime.getRuntime().exec( args.toArray( new String[args.size()] ) );
        final ProcessStreamHandler processOutput = new ProcessStreamHandler( process, false );
        processOutput.launch();
        final Future<Integer> realFuture = FutureAdapter.processFuture( process );
        return new Future<Integer>()
        {
            @Override
            public boolean cancel( boolean mayInterruptIfRunning )
            {
                try
                {
                    return realFuture.cancel( mayInterruptIfRunning );
                }
                finally
                {
                    processOutput.done();
                }
            }

            @Override
            public boolean isCancelled()
            {
                return realFuture.isCancelled();
            }

            @Override
            public boolean isDone()
            {
                return realFuture.isDone();
            }

            @Override
            public Integer get() throws InterruptedException, ExecutionException
            {
                try
                {
                    return realFuture.get();
                }
                finally
                {
                    processOutput.done();
                }
            }

            @Override
            public Integer get( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException,
                    TimeoutException
            {
                try
                {
                    return realFuture.get( timeout, unit );
                }
                finally
                {
                    processOutput.done();
                }
            }
        };
    }
}
