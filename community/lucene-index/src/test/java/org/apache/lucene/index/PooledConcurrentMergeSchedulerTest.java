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
package org.apache.lucene.index;

import org.apache.commons.lang3.RandomUtils;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.test.ThreadTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.mockito.Mockito.mock;

class PooledConcurrentMergeSchedulerTest
{

    private TestPooledConcurrentMergeScheduler mergeScheduler;
    private IndexWriter indexWriter = mock( IndexWriter.class );

    @BeforeEach
    void setUp()
    {
        mergeScheduler = new TestPooledConcurrentMergeScheduler();
    }

    @AfterEach
    void tearDown()
    {
        mergeScheduler.getExecutionLatch().countDown();
    }

    @Test
    void doNotAddMergeTaskWhenWriterDoesNotHaveMergesToDo() throws Exception
    {
        IndexWriter indexWriter = mock( IndexWriter.class );

        mergeScheduler.merge( indexWriter, MergeTrigger.EXPLICIT, false );

        assertEquals( 0, mergeScheduler.getWriterTaskCount() );
    }

    @Test
    void addMergeTaskWhenWriterHasOneMergeToPerform() throws IOException
    {
        SegmentCommitInfo segmentCommitInfo = getSegmentCommitInfo();

        Mockito.when( indexWriter.getNextMerge() ).thenReturn( new TestOneMerge( segmentCommitInfo ) )
                .thenReturn( null );

        mergeScheduler.merge( indexWriter, MergeTrigger.EXPLICIT, false );

        assertEquals( 1, mergeScheduler.getWriterTaskCount() );
    }

    @Test
    void addTwoMergeTasksWhenWriterHastwoMergeToPerform() throws IOException
    {
        SegmentCommitInfo segmentCommitInfo = getSegmentCommitInfo();

        Mockito.when( indexWriter.getNextMerge() ).thenReturn( new TestOneMerge( segmentCommitInfo ) )
                .thenReturn( new TestOneMerge( segmentCommitInfo ) ).thenReturn( null );

        mergeScheduler.merge( indexWriter, MergeTrigger.EXPLICIT, false );

        assertEquals( 2, mergeScheduler.getWriterTaskCount() );
    }

    @Test
    void writerCloseWaitForMergesInMergeQueue()
    {
        assertTimeout( Duration.ofSeconds( 10 ), () ->
        {
            indexWriter = mock( IndexWriter.class );
            SegmentCommitInfo segmentCommitInfo = getSegmentCommitInfo();

            Mockito.when( indexWriter.getNextMerge() ).thenReturn( new TestOneMerge( segmentCommitInfo ) ).thenReturn( null );

            mergeScheduler.merge( indexWriter, MergeTrigger.EXPLICIT, false );

            assertEquals( 1, mergeScheduler.getWriterTaskCount() );

            Thread closeSchedulerThread = ThreadTestUtils.fork( () -> mergeScheduler.close() );
            ThreadTestUtils.awaitThreadState( closeSchedulerThread, TimeUnit.SECONDS.toMillis( 5 ), Thread.State.TIMED_WAITING );
            mergeScheduler.getExecutionLatch().countDown();
            closeSchedulerThread.join();

            assertEquals( 0, mergeScheduler.getWriterTaskCount() );
        } );
    }

    private static SegmentCommitInfo getSegmentCommitInfo()
    {
        SegmentInfo segmentInfo =
                new SegmentInfo( mock( Directory.class ), Version.LATEST, "test", Integer.MAX_VALUE, true,
                        mock( Codec.class ), MapUtil.stringMap(), RandomUtils.nextBytes( 16 ), MapUtil.stringMap() );
        return new SegmentCommitInfo( segmentInfo, 1, 1L, 1L, 1L );
    }

    private static class TestPooledConcurrentMergeScheduler extends PooledConcurrentMergeScheduler
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

        class BlockingMerge extends ConcurrentMergeScheduler.MergeThread
        {

            private CountDownLatch executionLatch;

            BlockingMerge( IndexWriter writer, MergePolicy.OneMerge merge, CountDownLatch executionLatch )
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

    private static class TestOneMerge extends MergePolicy.OneMerge
    {
        TestOneMerge( SegmentCommitInfo segmentCommitInfo )
        {
            super( Collections.singletonList( segmentCommitInfo ) );
        }
    }
}
