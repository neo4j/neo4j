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
package org.neo4j.tracers;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.ToLongFunction;
import javax.annotation.Resource;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Cancelable;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorCounters;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.Runtime.getRuntime;
import static java.time.Duration.ofMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.ThreadLocalRandom.current;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.RandomStringUtils.random;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertTimeout;

@ExtendWith( TestDirectoryExtension.class )
public class PageCacheCountersIT
{
    @Resource
    public TestDirectory testDirectory;
    private GraphDatabaseService db;
    private ExecutorService executors;
    private int numberOfWorkers;

    @BeforeEach
    public void setUp()
    {
        db = new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.graphDbDir() );
        numberOfWorkers = getRuntime().availableProcessors();
        executors = newFixedThreadPool( numberOfWorkers );
    }

    @AfterEach
    public void tearDown() throws InterruptedException
    {
        executors.shutdown();
        executors.awaitTermination( 5, SECONDS );
        db.shutdown();
    }

    @RepeatedTest( 5 )
    public void pageCacheCountersAreSumOfPageCursorCounters() throws Exception
    {
        assertTimeout( ofMillis( 60_000 ), () -> {
            List<NodeCreator> nodeCreators = new ArrayList<>( numberOfWorkers );
            List<Future> nodeCreatorFutures = new ArrayList<>( numberOfWorkers );
            PageCacheTracer pageCacheTracer = getPageCacheTracer( db );

            long initialPins = pageCacheTracer.pins();
            long initialHits = pageCacheTracer.hits();
            long initialUnpins = pageCacheTracer.unpins();
            long initialBytesRead = pageCacheTracer.bytesRead();
            long initialBytesWritten = pageCacheTracer.bytesWritten();
            long initialEvictions = pageCacheTracer.evictions();
            long initialFaults = pageCacheTracer.faults();
            long initialFlushes = pageCacheTracer.flushes();

            startNodeCreators( nodeCreators, nodeCreatorFutures );
            while ( pageCacheTracer.pins() == 0 || pageCacheTracer.faults() == 0 || pageCacheTracer.unpins() == 0 )
            {
                MILLISECONDS.sleep( 10 );
            }
            stopNodeCreators( nodeCreators, nodeCreatorFutures );

            assertThat( "Number of pins events in page cache tracer should equal to the sum of pin events in " +
                            "page cursor tracers.", pageCacheTracer.pins(),
                    greaterThanOrEqualTo( sumCounters( nodeCreators, NodeCreator::getPins, initialPins ) ) );
            assertThat( "Number of unpins events in page cache tracer should equal to the sum of unpin events in " +
                            "page cursor tracers.", pageCacheTracer.unpins(),
                    greaterThanOrEqualTo( sumCounters( nodeCreators, NodeCreator::getUnpins, initialUnpins ) ) );
            assertThat( "Number of initialBytesRead in page cache tracer should equal to the sum of initialBytesRead " +
                            "in page cursor tracers.", pageCacheTracer.bytesRead(),
                    greaterThanOrEqualTo( sumCounters( nodeCreators, NodeCreator::getBytesRead, initialBytesRead ) ) );
            assertThat( "Number of bytesWritten in page cache tracer should equal to the sum of bytesWritten in " +
                            "page cursor tracers.", pageCacheTracer.bytesWritten(),
                    greaterThanOrEqualTo( sumCounters( nodeCreators, NodeCreator::getBytesWritten, initialBytesWritten ) ) );
            assertThat( "Number of evictions in page cache tracer should equal to the sum of evictions in " +
                            "page cursor tracers.", pageCacheTracer.evictions(),
                    greaterThanOrEqualTo( sumCounters( nodeCreators, NodeCreator::getEvictions, initialEvictions ) ) );
            assertThat(
                    "Number of faults in page cache tracer should equal to the sum of faults in page cursor tracers.",
                    pageCacheTracer.faults(),
                    greaterThanOrEqualTo( sumCounters( nodeCreators, NodeCreator::getFaults, initialFaults ) ) );
            assertThat(
                    "Number of flushes in page cache tracer should equal to the sum of flushes in page cursor tracers.",
                    pageCacheTracer.flushes(),
                    greaterThanOrEqualTo( sumCounters( nodeCreators, NodeCreator::getFlushes, initialFlushes ) ) );
            assertThat( "Number of hits in page cache tracer should equal to the sum of hits in page cursor tracers.",
                    pageCacheTracer.hits(),
                    greaterThanOrEqualTo( sumCounters( nodeCreators, NodeCreator::getHits, initialHits ) ) );
        } );
    }

    private void stopNodeCreators( List<NodeCreator> nodeCreators, List<Future> nodeCreatorFutures )
            throws InterruptedException, ExecutionException
    {
        nodeCreators.forEach( NodeCreator::cancel );
        for ( Future creatorFuture : nodeCreatorFutures )
        {
            creatorFuture.get();
        }
    }

    private void startNodeCreators( List<NodeCreator> nodeCreators, List<Future> nodeCreatorFutures )
    {
        for ( int i = 0; i < numberOfWorkers; i++ )
        {
            NodeCreator nodeCreator = new NodeCreator( db );
            nodeCreators.add( nodeCreator );
            nodeCreatorFutures.add( executors.submit( nodeCreator ) );
        }
    }

    private long sumCounters( List<NodeCreator> nodeCreators, ToLongFunction<NodeCreator> mapper, long initialValue )
    {
        return nodeCreators.stream().mapToLong( mapper ).sum() + initialValue;
    }

    private PageCacheTracer getPageCacheTracer( GraphDatabaseService db )
    {
        Tracers tracers = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( Tracers.class );
        return tracers.pageCacheTracer;
    }

    private class NodeCreator implements Runnable, Cancelable
    {
        private volatile boolean canceled;

        private final GraphDatabaseService db;
        private long pins;
        private long unpins;
        private long hits;
        private long bytesRead;
        private long bytesWritten;
        private long evictions;
        private long faults;
        private long flushes;

        NodeCreator( GraphDatabaseService db )
        {
            this.db = db;
        }

        @Override
        public void run()
        {
            ThreadLocalRandom localRandom = current();
            while ( !canceled )
            {
                PageCursorCounters pageCursorCounters;
                try ( Transaction transaction = db.beginTx(); KernelStatement kernelStatement = getKernelStatement( (GraphDatabaseAPI) db ) )
                {
                    pageCursorCounters = kernelStatement.getPageCursorTracer();
                    Node node = db.createNode();
                    node.setProperty( "name", random( localRandom.nextInt( 100 ) ) );
                    node.setProperty( "surname", random( localRandom.nextInt( 100 ) ) );
                    node.setProperty( "age", localRandom.nextInt( 100 ) );
                    transaction.success();
                    storeCounters( pageCursorCounters );
                }
            }
        }

        private void storeCounters( PageCursorCounters pageCursorCounters )
        {
            requireNonNull( pageCursorCounters );
            pins += pageCursorCounters.pins();
            unpins += pageCursorCounters.unpins();
            hits += pageCursorCounters.hits();
            bytesRead += pageCursorCounters.bytesRead();
            bytesWritten += pageCursorCounters.bytesWritten();
            evictions += pageCursorCounters.evictions();
            faults += pageCursorCounters.faults();
            flushes += pageCursorCounters.flushes();
        }

        @Override
        public void cancel()
        {
            canceled = true;
        }

        long getPins()
        {
            return pins;
        }

        long getUnpins()
        {
            return unpins;
        }

        public long getHits()
        {
            return hits;
        }

        long getBytesRead()
        {
            return bytesRead;
        }

        long getBytesWritten()
        {
            return bytesWritten;
        }

        long getEvictions()
        {
            return evictions;
        }

        long getFaults()
        {
            return faults;
        }

        long getFlushes()
        {
            return flushes;
        }

        private KernelStatement getKernelStatement( GraphDatabaseAPI db )
        {
            ThreadToStatementContextBridge statementBridge =
                    db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
            return (KernelStatement) statementBridge.get();
        }
    }
}
