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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.locks.ReentrantLock;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigMode;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSettings;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSettings.PostPopulationCompaction;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.schema.populator.LuceneIndexPopulator;
import org.neo4j.kernel.api.index.IndexEntryConflictHandler;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.util.concurrent.BinaryLatch;

class VectorIndexPopulator extends LuceneIndexPopulator<DatabaseIndex<VectorIndexReader>> {
    private final VectorDocumentStructure documentStructure;
    private final Neo4jVectorSimilarityFunction similarityFunction;
    private final Config config;
    private final IndexMonitor monitor;

    private enum CompactionState {
        NOT_STARTED,
        RUNNING,
        DONE
    }

    // Volatile, not guarded by this monitor: the compacting thread polls these while drop/close hold the monitor
    // waiting for that same thread to finish, so reading them must never need the monitor.
    private volatile boolean cancelled;
    private volatile CompactionState compactionState = CompactionState.NOT_STARTED;
    private final BinaryLatch compactionDone = new BinaryLatch();

    // Held while the lucene index is torn down, and while the merge abort reads its partitions, so the two cannot
    // overlap: the partition walk throws if the index closes underneath it.
    private final ReentrantLock indexTeardownLock = new ReentrantLock();

    VectorIndexPopulator(
            DatabaseIndex<VectorIndexReader> luceneIndex,
            IndexUpdateIgnoreStrategy ignoreStrategy,
            VectorDocumentStructure documentStructure,
            Neo4jVectorSimilarityFunction similarityFunction,
            Config config,
            IndexMonitor monitor) {
        super(luceneIndex, ignoreStrategy);
        this.documentStructure = documentStructure;
        this.similarityFunction = similarityFunction;
        this.config = config;
        this.monitor = monitor;
    }

    @Override
    public IndexUpdater newPopulatingUpdater(CursorContext cursorContext) {
        return new VectorIndexPopulatingUpdater(writer, ignoreStrategy, documentStructure, similarityFunction);
    }

    /**
     * Compact the freshly populated segments before the index is marked online. Running the merge here, rather
     * than from {@link VectorIndexProvider#getOnlineAccessor}, means it executes on the still-open populating
     * writer which is configured with the parallel intra-merge codec — the online accessor's codec forces the
     * merge to be single-threaded.
     * <p/>
     * This deliberately happens in {@code scanCompleted} rather than in {@link #close(boolean, CursorContext)}:
     * {@code close} is invoked from inside {@code FlippableIndexProxy.flip}, which holds that proxy's exclusive
     * lock, so compacting there stalled every reader and writer of the index - including {@code SHOW INDEXES} -
     * for the entire duration of the merge, which can be hours on a large index. {@code scanCompleted} is called
     * before the flip and holds no such lock.
     */
    @Override
    public void scanCompleted(
            PhaseTracker phaseTracker,
            PopulationWorkScheduler populationWorkScheduler,
            IndexEntryConflictHandler conflictHandler,
            CursorContext cursorContext) {
        if (!markCompactionStarted()) {
            // Already dropped or stopped, so this index is going away - don't start compacting it.
            return;
        }
        try {
            phaseTracker.enterPhase(PhaseTracker.Phase.MERGE);
            monitor.postPopulationCompactionStarted(luceneIndex.getDescriptor());
            compactSegments();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            compactionState = CompactionState.DONE;
            compactionDone.release();
        }
    }

    @Override
    public synchronized void drop() {
        indexTeardownLock.lock();
        try {
            abandonMerging();
            super.drop();
        } finally {
            indexTeardownLock.unlock();
        }
    }

    @Override
    public synchronized void close(boolean populationCompletedSuccessfully, CursorContext cursorContext) {
        indexTeardownLock.lock();
        try {
            if (populationCompletedSuccessfully) {
                // Compaction ran in scanCompleted on this same thread, so it has already finished. Wait rather than
                // abort, so that the final commit is free to merge as usual.
                awaitCompaction();
            } else {
                abandonMerging();
            }
            super.close(populationCompletedSuccessfully, cursorContext);
        } finally {
            indexTeardownLock.unlock();
        }
    }

    private synchronized boolean markCompactionStarted() {
        if (cancelled) {
            return false;
        }
        compactionState = CompactionState.RUNNING;
        return true;
    }

    /**
     * Deliberately not synchronized, and deliberately does not wait: this is called by the thread stopping the
     * population, which then waits for the population job to finish. Blocking here - either on this populator's
     * monitor or on the merging itself - would reintroduce exactly the wait it exists to avoid. The subsequent
     * {@link #close(boolean, CursorContext)} is what waits for the merges to actually stop.
     * <p/>
     * The {@code tryLock} is what keeps the abort off the lucene index while {@link #drop()} or
     * {@link #close(boolean, CursorContext)} is tearing it down: reading its partitions then throws. Skipping the
     * abort in that case loses nothing, since drop and an unsuccessful close request it themselves and a successful
     * close deliberately lets its final merges finish. The cancellation flag is still set unconditionally, so a
     * compaction that has not started yet never starts.
     */
    @Override
    public void cancelPostScanWork() {
        cancelled = true;
        if (indexTeardownLock.tryLock()) {
            try {
                requestMergeAbort();
            } finally {
                indexTeardownLock.unlock();
            }
        }
    }

    /**
     * Abandon all merging on this index, then wait for an in-flight compaction to notice. Called while holding this
     * populator's monitor, which is safe because {@code scanCompleted} only holds that monitor to start the
     * compaction, not while running it.
     */
    private void abandonMerging() {
        if (requestMergeAbort()) {
            awaitCompaction();
        }
    }

    /**
     * Ask every merge on this index to give up, whether it belongs to post-population compaction or to the ordinary
     * merging the populating writer does as it ingests.
     * <p/>
     * Aborting the latter matters just as much: closing the writer - which is what dropping or stopping the index
     * does - ends in {@code IndexWriter.waitForMerges()}, so an ordinary merge left running holds the drop up for
     * exactly as long as it takes to finish. That was measured at 100s of a 122s wall-clock profile, and it happens
     * throughout population rather than only in the compaction phase at the end.
     *
     * @return whether a <em>compaction</em> specifically was in flight, and so is worth waiting for afterwards.
     */
    private boolean requestMergeAbort() {
        // Setting this before reading the state is what stops a compaction from starting after this point:
        // markCompactionStarted() checks it, so at worst we miss an in-flight compaction that is only just beginning
        // and it stops itself at the first check instead.
        cancelled = true;

        boolean compacting = compactionState == CompactionState.RUNNING;
        if (compacting) {
            // Reported only from here, so observing it proves a compaction really was in flight at the time
            monitor.postPopulationCompactionAborted(luceneIndex.getDescriptor());
        }

        if (luceneIndex.isOpen()) {
            // Not open means the index was never created, so there is nothing merging
            for (AbstractIndexPartition partition : luceneIndex.getPartitions()) {
                partition.getIndexWriter().abortMerges();
            }
        }
        return compacting;
    }

    private void awaitCompaction() {
        if (compactionState == CompactionState.NOT_STARTED) {
            // Nothing will ever release the latch, so there is nothing to wait for
            return;
        }
        // Closing the Lucene index underneath a running merge is not safe, so this wait is what keeps drop and
        // shutdown correct. Aborting the merges above is only there to keep it short.
        compactionDone.await();
    }

    @Override
    protected boolean usesSeparateDocuments() {
        return true;
    }

    @Override
    protected LuceneDocument updateAsDocument(ValueIndexEntryUpdate update) {
        return documentsFactory.createVectorDocument(
                documentStructure, update.getEntityId(), similarityFunction, update.values());
    }

    /**
     * Perform post-population compaction of the index according to
     * {@link LuceneSettings#vector_post_population_compaction}.
     * <ul>
     *   <li>{@link PostPopulationCompaction#NONE} — skip merging entirely.</li>
     *   <li>{@link PostPopulationCompaction#AUTO} — invoke Lucene's natural merge policy via
     *       {@code maybeMerge()}. The configured merge policy decides which segments (if any) to merge.</li>
     *   <li>{@link PostPopulationCompaction#PARTIAL} — force-merge down to
     *       {@link LuceneSettings#vector_standard_merge_factor} segments per partition.</li>
     *   <li>{@link PostPopulationCompaction#FULL} — force-merge each partition to a single segment.</li>
     * </ul>
     */
    private void compactSegments() throws IOException {
        PostPopulationCompaction mode = config.get(LuceneSettings.vector_post_population_compaction);
        if (mode == PostPopulationCompaction.NONE) {
            return;
        }
        // The populating writer is configured with the population merge tuning (e.g. a large mergeFactor) which
        // suppresses merging during ingest. Swap in the standard tuning so AUTO's maybeMerge() actually
        // consolidates rather than no-opping. Harmless for PARTIAL/FULL, which forceMerge past the policy anyway.
        IndexWriterConfigMode standard = IndexWriterConfigMode.VECTOR;
        int forceMergeTarget = mode == PostPopulationCompaction.PARTIAL ? standard.getMergeFactor(config) : 1;

        IOException exception = null;
        for (AbstractIndexPartition partition : luceneIndex.getPartitions()) {
            if (cancelled) {
                break;
            }
            try {
                LuceneIndexWriter writer = partition.getIndexWriter();
                writer.updateMergePolicy(
                        standard.getMergeFactor(config),
                        standard.segmentsPerTier(config),
                        standard.maxMergeAtOnce(config));
                switch (mode) {
                    case NONE -> {
                        // handled above
                    }
                    case AUTO -> writer.maybeMerge();
                    case PARTIAL, FULL -> writer.forceMerge(forceMergeTarget);
                }
            } catch (IOException e) {
                if (cancelled) {
                    // We asked this merge to abandon its work, so this is the expected way out rather than a
                    // population failure. Lucene swallows the abort itself for maybeMerge(), but forceMerge()
                    // reports it back through the writer's recorded merge exceptions.
                    break;
                }
                if (exception != null) {
                    exception.addSuppressed(e);
                } else {
                    exception = e;
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }
}
