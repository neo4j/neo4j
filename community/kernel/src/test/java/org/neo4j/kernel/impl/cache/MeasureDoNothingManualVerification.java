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
package org.neo4j.kernel.impl.cache;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Log;

import static java.util.Arrays.asList;

public class MeasureDoNothingManualVerification
{
    /**
     * MeasureDoNothing depends on full-thread pauses to validate that it works, which frankly is hard to do in a test.
     * We could mock out the wait() call, but that is the exact thing we'd want to test - does it in fact catch full
     * thread pauses?
     *
     * So, instead, here's a manual method for causing a log of garbage and tracking the logging that comes out.
     */

    public static void main(String ... args) throws InterruptedException, ExecutionException
    {
        Log log = FormattedLog.toOutputStream( System.out );
        new Thread(new MeasureDoNothing( "GC Monitor", log, 100, 1 )).start();

        ExecutorService executorService = Executors.newFixedThreadPool( 4 );
        for ( Future<Object> objectFuture : executorService.invokeAll(
                asList( new GCHeavyJob(),new GCHeavyJob(),new GCHeavyJob(),new GCHeavyJob() ) ) )
        {
            objectFuture.get();
        }
    }

    private static class GCHeavyJob implements Callable<Object>
    {
        Map<String, Object> objects = new ConcurrentHashMap<>();
        Random rand = new Random();

        @Override
        public Object call() throws Exception
        {
            while(true)
            {
                objects.put("key" + rand.nextInt(1_000), ByteBuffer.allocate( 8192 ));
                if(false)
                {
                    break;
                }
            }
            return null;
        }
    }
}
