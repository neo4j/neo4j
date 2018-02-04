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
package org.apache.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Thread.State.TIMED_WAITING;
import static java.time.Duration.ofMillis;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.RandomUtils.nextBytes;
import static org.apache.lucene.index.MergePolicy.OneMerge;
import static org.apache.lucene.index.MergeTrigger.EXPLICIT;
import static org.apache.lucene.util.Version.LATEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.ThreadTestUtils.awaitThreadState;
import static org.neo4j.test.ThreadTestUtils.fork;

public class PooledConcurrentMergeSchedulerTest
{

    private TestPooledConcurrentMergeScheduler mergeScheduler;
    private IndexWriter indexWriter = mock( IndexWriter.class );

    @BeforeEach
    public void setUp() throws Exception
    {
        mergeScheduler = new TestPooledConcurrentMergeScheduler();
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        mergeScheduler.getExecutionLatch().countDown();
    }

    @Test
    public void doNotAddMergeTaskWhenWriterDoesNotHaveMergesToDo() throws Exception
    {
        IndexWriter indexWriter = mock( IndexWriter.class );

        mergeScheduler.merge( indexWriter, EXPLICIT, false );

        assertEquals( 0, mergeScheduler.getWriterTaskCount() );
    }

    @Test
    public void addMergeTaskWhenWriterHasOneMergeToPerform() throws IOException
    {
        SegmentCommitInfo segmentCommitInfo = getSegmentCommitInfo();

        when( indexWriter.getNextMerge() ).thenReturn( new TestOneMerge( segmentCommitInfo ) )
                .thenReturn( null );

        mergeScheduler.merge( indexWriter, EXPLICIT, false );

        assertEquals( 1, mergeScheduler.getWriterTaskCount() );
    }

    @Test
    public void addTwoMergeTasksWhenWriterHastwoMergeToPerform() throws IOException
    {
        SegmentCommitInfo segmentCommitInfo = getSegmentCommitInfo();

        when( indexWriter.getNextMerge() ).thenReturn( new TestOneMerge( segmentCommitInfo ) )
                .thenReturn( new TestOneMerge( segmentCommitInfo ) ).thenReturn( null );

        mergeScheduler.merge( indexWriter, EXPLICIT, false );

        assertEquals( 2, mergeScheduler.getWriterTaskCount() );
    }

    @Test
    public void writerCloseWaitForMergesInMergeQueue() throws IOException
    {
        assertTimeout( ofMillis( 10_000 ), () -> {
            indexWriter = mock( IndexWriter.class );
            SegmentCommitInfo segmentCommitInfo = getSegmentCommitInfo();

            when( indexWriter.getNextMerge() ).thenReturn( new TestOneMerge( segmentCommitInfo ) ).thenReturn( null );

            mergeScheduler.merge( indexWriter, EXPLICIT, false );

            assertEquals( 1, mergeScheduler.getWriterTaskCount() );

            Thread closeSchedulerThread = fork( () -> mergeScheduler.close() );
            awaitThreadState( closeSchedulerThread, SECONDS.toMillis( 5 ), TIMED_WAITING );
            mergeScheduler.getExecutionLatch().countDown();
            closeSchedulerThread.join();

            assertEquals( 0, mergeScheduler.getWriterTaskCount() );
        } );
    }

    private SegmentCommitInfo getSegmentCommitInfo()
    {
        SegmentInfo segmentInfo =
                new SegmentInfo( mock( Directory.class ), LATEST, "test", MAX_VALUE, true,
                        mock( Codec.class ), stringMap(), nextBytes( 16 ), stringMap() );
        return new SegmentCommitInfo( segmentInfo, 1, 1L, 1L, 1L );
    }

    private class TestPooledConcurrentMergeScheduler extends PooledConcurrentMergeScheduler
    {

        private CountDownLatch executionLatch = new CountDownLatch( 1 );

        @Override
        protected synchronized MergeThread getMergeThread( IndexWriter writer, MergePolicy.OneMerge merge )
        {
            return new BlockingMerge( writer, merge, executionLatch );
        }

        CountDownLatch getExecutionLatch()
        {
            return executionLatch;
        }

        class BlockingMerge extends MergeThread
        {

            private CountDownLatch executionLatch;

            BlockingMerge( IndexWriter writer, OneMerge merge, CountDownLatch executionLatch )
            {
                super( writer, merge );
                this.executionLatch = executionLatch;
            }

            @Override
            public void run()
            {
                try
                {
                    executionLatch.await();
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( "Interrupted while waiting for a latch", e );
                }
            }
        }
    }

    private class TestOneMerge extends OneMerge
    {

        TestOneMerge( SegmentCommitInfo segmentCommitInfo )
        {
            super( singletonList( segmentCommitInfo ) );
        }
    }
}
