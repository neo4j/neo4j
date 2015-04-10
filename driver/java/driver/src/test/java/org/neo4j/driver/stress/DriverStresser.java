/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.stress;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.Neo4j;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.util.Neo4jRunner;

public class DriverStresser
{

    private static Neo4jRunner server;

    public static void main( String... args ) throws Throwable
    {
        int iterations = 100_000;

        bench( (long) iterations, 1, 10_000 );
        bench( (long) iterations / 2, 2, 10_000 );
        bench( (long) iterations / 4, 4, 10_000 );
        bench( (long) iterations / 8, 8, 10_000 );
        bench( (long) iterations / 16, 16, 10_000 );
        bench( (long) iterations / 32, 32, 10_000 );
    }

    public static void setup() throws Exception
    {
        server = new Neo4jRunner();
        server.startServer();
    }

    static class Worker
    {
        private final Session session;

        public Worker()
        {
            session = Neo4j.session( "neo4j://localhost" );
        }

        public int operation()
        {
            String statement = "RETURN 1 AS n";                   // = "CREATE (a {name:{n}}) RETURN a.name";
            Map<String,Value> parameters = Neo4j.parameters();  // = Neo4j.parameters( "n", "Bob" );

            int total = 0;
            Result result = session.run( statement, parameters );
            while ( result.next() )
            {
                total += result.get( "n" ).javaInteger();
            }
            return total;
        }
    }

    public static void teardown() throws InterruptedException
    {
        server.stopServer();
    }


    private static void bench( long iterations, int concurrency, long warmupIterations ) throws Exception
    {
        ExecutorService executorService = Executors.newFixedThreadPool( concurrency );

        setup();

        // Warmup
        awaitAll( executorService.invokeAll( workers( warmupIterations, concurrency ) ) );

        long start = System.nanoTime();
        List<Future<Object>> futures = executorService.invokeAll( workers( iterations, concurrency ) );
        awaitAll( futures );
        long delta = System.nanoTime() - start;

        System.out.println(
                "With " + concurrency + " threads: " + (iterations * concurrency) / (delta / 1_000_000_000.0) +
                " ops/s" );

        teardown();

        executorService.shutdownNow();
        executorService.awaitTermination( 10, TimeUnit.SECONDS );
    }

    private static void awaitAll( List<Future<Object>> futures ) throws Exception
    {
        for ( Future<Object> future : futures )
        {
            future.get();
        }
    }

    private static List<Callable<Object>> workers( final long iterations, final int numWorkers )
    {
        List<Callable<Object>> workers = new ArrayList<>();
        for ( int i = 0; i < numWorkers; i++ )
        {
            final Worker worker = new Worker();
            workers.add( new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    int dontRemoveMyCode = 0;
                    for ( int i = 0; i < iterations; i++ )
                    {
                        dontRemoveMyCode += worker.operation();
                    }
                    return dontRemoveMyCode;
                }
            } );
        }
        return workers;
    }
}
