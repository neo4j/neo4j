/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;

public class TxBench
{
    public static void main(String ... args) throws Throwable
    {
        int iterations = 500_000;

        bench( (long) iterations, 1 );
        bench( (long) iterations / 2, 2 );
        bench( (long) iterations / 4, 4 );
        bench( (long) iterations / 8, 8 );
    }


    private static GraphDatabaseService db;

    public static void setup() throws IOException
    {
        File path = new File( "/tmp/asd" );
        if( path.exists())
        {
            FileUtils.deleteRecursively( path );
        }
        db = new GraphDatabaseFactory().newEmbeddedDatabase( path.getAbsolutePath() );
        try( Transaction tx = db.beginTx() )
        {
            db.execute( "CREATE INDEX ON :User(name)" );
            tx.success();
        }

        try( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.success();
        }
    }

    static class Worker
    {
        private final Map<String, Object> params = new HashMap<>();

        public void operation()
        {
            try ( Transaction tx = db.beginTx() )
            {
                params.put( "name", ThreadLocalRandom.current().nextInt( 100000 ) );
                db.execute( "MERGE (n:User {name:{name}}) RETURN n", params );
                tx.success();
            }
        }
    }

    public static void teardown()
    {
        db.shutdown();
    }


    private static void bench( long iterations, int concurrency ) throws InterruptedException, ExecutionException,
            IOException
    {
        ExecutorService executorService = Executors.newFixedThreadPool( concurrency );

        setup();

        long start = System.nanoTime();
        List<Future<Object>> futures = executorService.invokeAll( workers( iterations, concurrency ) );
        awaitAll( futures );
        long delta = System.nanoTime() - start;

        System.out.println("With "+ concurrency +" threads: " + (iterations * concurrency) / (delta / 1000_000_000.0) + " ops/s");

        teardown();

        executorService.shutdownNow();
        executorService.awaitTermination( 10, TimeUnit.SECONDS );
    }

    private static void awaitAll( List<Future<Object>> futures ) throws InterruptedException, ExecutionException
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
                    for ( int i = 0; i < iterations; i++ )
                    {
                        worker.operation();
                    }
                    return null;
                }
            });
        }
        return workers;
    }

}
