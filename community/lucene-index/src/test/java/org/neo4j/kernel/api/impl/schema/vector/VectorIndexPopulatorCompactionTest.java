/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.schema.vector;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSettings;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSettings.PostPopulationCompaction;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.test.OtherThreadExecutor;

/**
 * Post-population compaction of a vector index runs a Lucene merge that takes minutes on a large index. These tests
 * pin down where it runs and that it can be abandoned, because both matter well beyond this class: running it during
 * the flip stalled every reader and writer of the index, and being unable to abandon it would make dropping an index
 * or shutting down the database wait out the whole merge.
 */
class VectorIndexPopulatorCompactionTest {
    private DatabaseIndex<VectorIndexReader> luceneIndex;
    private LuceneIndexWriter writer;
    private VectorIndexPopulator populator;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        luceneIndex = mock(DatabaseIndex.class);
        writer = mock(LuceneIndexWriter.class);
        AbstractIndexPartition partition = mock(AbstractIndexPartition.class);
        when(partition.getIndexWriter()).thenReturn(writer);
        when(luceneIndex.getPartitions()).thenReturn(List.of(partition));
        // The populator only reaches for partitions on an index that has been created and opened
        when(luceneIndex.isOpen()).thenReturn(true);

        Config config =
                Config.defaults(LuceneSettings.vector_post_population_compaction, PostPopulationCompaction.AUTO);
        // The document structure and similarity function play no part in compaction
        populator = new VectorIndexPopulator(
                luceneIndex, IndexUpdateIgnoreStrategy.NO_IGNORE, null, null, config, IndexMonitor.NO_MONITOR);
    }

    @Test
    void shouldCompactInScanCompletedRatherThanInClose() throws Exception {
        // WHEN
        scanCompleted();

        // THEN the merge has already happened by the time of the flip, which is what calls close()
        verify(writer).maybeMerge();

        // and close() itself does not merge, so it does not hold the flip lock for the merge duration
        populator.close(true, NULL_CONTEXT);
        verify(writer).maybeMerge();
    }

    @Test
    void shouldNotStartCompactingWhenAlreadyDropped() throws Exception {
        // GIVEN an index dropped before the scan completed
        populator.drop();

        // WHEN
        scanCompleted();

        // THEN there is no point compacting an index that is going away
        verify(writer, never()).maybeMerge();
    }

    /**
     * The populating writer merges as it ingests, long before the compaction phase. Closing the writer - which is what
     * dropping does - ends in {@code IndexWriter.waitForMerges()}, so leaving one of those merges running holds the
     * drop up for as long as the merge takes. Observed at 100s on a real index.
     */
    @Test
    void shouldAbortMergesOnDropEvenWithNoCompactionInFlight() {
        // GIVEN a population still ingesting, so scanCompleted has never run
        // WHEN
        populator.drop();

        // THEN merging is still stopped, so closing the writer does not wait for it
        verify(writer).abortMerges();
    }

    @Test
    void shouldAbortMergesWhenPopulationIsStoppedWithNoCompactionInFlight() {
        populator.close(false, NULL_CONTEXT);

        verify(writer).abortMerges();
    }

    @Test
    void shouldNotAbortMergesOnSuccessfulClose() {
        // A successful population wants its final commit to merge as usual
        populator.close(true, NULL_CONTEXT);

        verify(writer, never()).abortMerges();
    }

    @Test
    void shouldAbortInFlightCompactionOnDropAndWaitForIt() throws Exception {
        // GIVEN a compaction that has started and is still running
        CountDownLatch mergeStarted = new CountDownLatch(1);
        CountDownLatch releaseMerge = new CountDownLatch(1);
        doAnswer(invocation -> {
                    mergeStarted.countDown();
                    assertTrue(releaseMerge.await(10, SECONDS));
                    return null;
                })
                .when(writer)
                .maybeMerge();

        try (OtherThreadExecutor compactingThread = new OtherThreadExecutor("Compacting thread");
                OtherThreadExecutor droppingThread = new OtherThreadExecutor("Dropping thread")) {
            Future<Void> compaction = compactingThread.executeDontWait(() -> {
                scanCompleted();
                return null;
            });
            assertTrue(mergeStarted.await(10, SECONDS));

            // WHEN the index is dropped mid-merge
            Future<Void> drop = droppingThread.executeDontWait(() -> {
                populator.drop();
                return null;
            });

            // THEN the merge is asked to abandon its work...
            verifyEventually(() -> verify(writer).abortMerges());

            // ...and the drop waits for it rather than closing the index underneath a running merge
            assertThat(drop.isDone()).isFalse();

            // Stand in for the merge noticing the abort
            releaseMerge.countDown();
            compaction.get(10, SECONDS);
            drop.get(10, SECONDS);
            verify(luceneIndex).drop();
        }
    }

    /**
     * The thread stopping the population and the population thread itself meet on the same lucene index: the abort
     * walks that index's partitions, and the walk throws as soon as the population thread has closed it.
     */
    @Test
    void shouldNotReadIndexPartitionsWhileIndexIsBeingClosed() throws Exception {
        // GIVEN a population thread that has started closing the index
        CountDownLatch closeEntered = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);
        doAnswer(invocation -> {
                    closeEntered.countDown();
                    assertTrue(releaseClose.await(10, SECONDS));
                    return null;
                })
                .when(luceneIndex)
                .close();

        try (OtherThreadExecutor closingThread = new OtherThreadExecutor("Closing thread")) {
            Future<Void> close = closingThread.executeDontWait(() -> {
                populator.close(true, NULL_CONTEXT);
                return null;
            });
            assertTrue(closeEntered.await(10, SECONDS));

            // WHEN the population is stopped at that exact moment
            populator.cancelPostScanWork();

            // THEN the index being closed is left alone
            verify(luceneIndex, never()).getPartitions();

            releaseClose.countDown();
            close.get(10, SECONDS);
        }
    }

    private void scanCompleted() throws Exception {
        // The compaction schedules no work of its own, hence the null scheduler
        populator.scanCompleted(PhaseTracker.nullInstance, null, NULL_CONTEXT);
    }

    /**
     * The abort is issued from another thread, so allow for it not having landed yet rather than racing it.
     */
    private static void verifyEventually(Runnable verification) throws InterruptedException {
        AssertionError last = null;
        for (int i = 0; i < 1000; i++) {
            try {
                verification.run();
                return;
            } catch (AssertionError e) {
                last = e;
                Thread.sleep(10);
            }
        }
        throw last;
    }
}
