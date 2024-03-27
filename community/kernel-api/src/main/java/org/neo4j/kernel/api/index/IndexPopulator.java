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
package org.neo4j.kernel.api.index;

import static org.neo4j.internal.helpers.collection.Iterators.emptyResourceIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.api.index.SwallowingIndexUpdater;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;
import org.neo4j.storageengine.api.UpdateMode;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.values.storable.Value;

/**
 * Used for initial population of an index.
 */
public interface IndexPopulator extends MinimalIndexAccessor {
    IndexPopulator EMPTY = new Adapter();

    /**
     * Remove all data in the index and paves the way for populating an index.
     *
     * @throws UncheckedIOException on I/O error.
     */
    void create() throws IOException;

    /**
     * Called when initially populating an index over existing data. Can be called concurrently by multiple threads.
     * All data coming in here is guaranteed to not
     * have been added to this index previously, so no checks needs to be performed before applying it.
     * Implementations verify constraints at this time.
     *
     * @param updates batch of index updates (entity property updates or entity token updates) that needs to be inserted.
     * Depending on the type of index the updates will be  {@link ValueIndexEntryUpdate property value index updates}
     * or {@link TokenIndexEntryUpdate token index updates}.
     * @param cursorContext underlying page cache events tracer
     * @throws IndexEntryConflictException if this is a uniqueness index and any of the updates are detected
     * to violate that constraint.
     * @throws UncheckedIOException on I/O error.
     */
    void add(Collection<? extends IndexEntryUpdate<?>> updates, CursorContext cursorContext)
            throws IndexEntryConflictException;

    /**
     * Return an updater for applying a set of changes to this index, generally this will be a set of changes from a
     * transaction.
     *
     * Index population goes through the existing data in the graph and feeds relevant data to this populator.
     * Simultaneously as population progresses there might be incoming updates
     * from committing transactions, which needs to be applied as well. This populator will only receive updates
     * for entities that it already has seen. Updates coming in here must be applied idempotently as the same data
     * may have been {@link #add(Collection, CursorContext) added previously }.
     * Updates can come in three different {@link IndexEntryUpdate#updateMode()} modes}.
     * <ol>
     *   <li>{@link UpdateMode#ADDED} means that there's an added property/label/type to an entity already seen by this
     *   populator and so needs to be added. Note that this addition needs to be applied idempotently.
     *   <li>{@link UpdateMode#CHANGED} means that there's a change to a property/label/type for an entity already seen by
     *   this populator and that this new change needs to be applied. Note that this change needs to be
     *   applied idempotently.</li>
     *   <li>{@link UpdateMode#REMOVED} means that a property/label/type already seen by this populator or even the entity itself
     *   has been removed and need to be removed from this index as well. Note that this removal needs to be
     *   applied idempotently.</li>
     * </ol>
     *
     * @param cursorContext underlying page cache events tracer
     * @return an {@link IndexUpdater} which will funnel changes that happen concurrently with index population
     * into the population and incorporating them as part of the index population.
     */
    IndexUpdater newPopulatingUpdater(CursorContext cursorContext);

    /**
     * Close this populator and releases any resources related to it.
     * If {@code populationCompletedSuccessfully} is {@code true} then it must mark this index
     * as {@link InternalIndexState#ONLINE} so that future invocations of its parent
     * {@link IndexProvider#getInitialState(IndexDescriptor, CursorContext, ImmutableSet)} also returns {@link InternalIndexState#ONLINE}.
     *
     * @param populationCompletedSuccessfully {@code true} if the index population was successful, where the index should
     * be marked as {@link InternalIndexState#ONLINE}. Supplying {@code false} can have two meanings:
     * <ul>
     *     <li>if {@link #markAsFailed(String)} have been called the end state should be {@link InternalIndexState#FAILED}.
     *     This method call should also make sure that the failure message gets stored for retrieval the next open, and made available for later requests
     *     via {@link IndexProvider#getPopulationFailure(IndexDescriptor, CursorContext, ImmutableSet)}.</li>
     *     <li>if {@link #markAsFailed(String)} have NOT been called the end state should be {@link InternalIndexState#POPULATING}</li>
     * </ul>
     * @param cursorContext underlying page cache events tracer
     */
    void close(boolean populationCompletedSuccessfully, CursorContext cursorContext);

    /**
     * Called when a population failed. The failure string should be stored for future retrieval by
     * {@link IndexProvider#getPopulationFailure(IndexDescriptor, CursorContext, ImmutableSet)}. Called before {@link #close(boolean, CursorContext)}
     * if there was a failure during population.
     *
     * @param failure the description of the failure.
     * @throws UncheckedIOException if marking failed.
     */
    void markAsFailed(String failure);

    /**
     * Add the given {@link IndexEntryUpdate update} to the sampler for this index.
     *
     * @param update update to include in sample
     */
    void includeSample(IndexEntryUpdate<?> update);

    /**
     * @return {@link IndexSample} from samples collected by {@link #includeSample(IndexEntryUpdate)} calls.
     */
    IndexSample sample(CursorContext cursorContext);

    /**
     * Returns actual population progress, given the progress of the scan. This is for when a populator needs to do
     * significant work after scan has completed where the scan progress can be seen as only a part of the whole progress.
     * @param scanProgress progress of the scan.
     * @return progress of the population of this index as a whole.
     */
    default PopulationProgress progress(PopulationProgress scanProgress) {
        return scanProgress;
    }

    default void scanCompleted(
            PhaseTracker phaseTracker, PopulationWorkScheduler populationWorkScheduler, CursorContext cursorContext)
            throws IndexEntryConflictException {
        scanCompleted(phaseTracker, populationWorkScheduler, IndexEntryConflictHandler.THROW, cursorContext);
    }

    default void scanCompleted(
            PhaseTracker phaseTracker,
            PopulationWorkScheduler populationWorkScheduler,
            IndexEntryConflictHandler conflictHandler,
            CursorContext cursorContext)
            throws IndexEntryConflictException { // no-op by default
    }

    /**
     * A scheduler for delegating index population related jobs to other threads.
     */
    @FunctionalInterface
    interface PopulationWorkScheduler {

        <T> JobHandle<T> schedule(JobDescriptionSupplier descriptionSupplier, Callable<T> job);
    }

    /**
     * Supplier of a description of the submitted job.
     * <p>
     * The description of what the job does needs to be provided
     * for monitoring purposes.
     * It accepts an index name in case the implementation might want to
     * include that information in the created description.
     */
    @FunctionalInterface
    interface JobDescriptionSupplier {

        String getJobDescription(String indexName);
    }

    class Adapter implements IndexPopulator {
        @Override
        public void create() {}

        @Override
        public void drop() {}

        @Override
        public ResourceIterator<Path> snapshotFiles() {
            return emptyResourceIterator();
        }

        @Override
        public void add(Collection<? extends IndexEntryUpdate<?>> updates, CursorContext cursorContext) {}

        @Override
        public IndexUpdater newPopulatingUpdater(CursorContext cursorContext) {
            return SwallowingIndexUpdater.INSTANCE;
        }

        @Override
        public void scanCompleted(
                PhaseTracker phaseTracker,
                PopulationWorkScheduler jobScheduler,
                IndexEntryConflictHandler conflictHandler,
                CursorContext cursorContext) {}

        @Override
        public void close(boolean populationCompletedSuccessfully, CursorContext cursorContext) {}

        @Override
        public void markAsFailed(String failure) {}

        @Override
        public void includeSample(IndexEntryUpdate<?> update) {}

        @Override
        public IndexSample sample(CursorContext cursorContext) {
            return new IndexSample();
        }
    }

    class Delegating implements IndexPopulator {
        private final IndexPopulator delegate;

        public Delegating(IndexPopulator delegate) {
            this.delegate = delegate;
        }

        @Override
        public void create() throws IOException {
            delegate.create();
        }

        @Override
        public void drop() {
            delegate.drop();
        }

        @Override
        public ResourceIterator<Path> snapshotFiles() throws IOException {
            return delegate.snapshotFiles();
        }

        @Override
        public void add(Collection<? extends IndexEntryUpdate<?>> updates, CursorContext cursorContext)
                throws IndexEntryConflictException {
            delegate.add(updates, cursorContext);
        }

        @Override
        public IndexUpdater newPopulatingUpdater(CursorContext cursorContext) {
            return delegate.newPopulatingUpdater(cursorContext);
        }

        @Override
        public void close(boolean populationCompletedSuccessfully, CursorContext cursorContext) {
            delegate.close(populationCompletedSuccessfully, cursorContext);
        }

        @Override
        public void markAsFailed(String failure) {
            delegate.markAsFailed(failure);
        }

        @Override
        public void includeSample(IndexEntryUpdate<?> update) {
            delegate.includeSample(update);
        }

        @Override
        public IndexSample sample(CursorContext cursorContext) {
            return delegate.sample(cursorContext);
        }

        @Override
        public PopulationProgress progress(PopulationProgress scanProgress) {
            return delegate.progress(scanProgress);
        }

        @Override
        public void scanCompleted(
                PhaseTracker phaseTracker,
                PopulationWorkScheduler jobScheduler,
                IndexEntryConflictHandler conflictHandler,
                CursorContext cursorContext)
                throws IndexEntryConflictException {
            delegate.scanCompleted(phaseTracker, jobScheduler, conflictHandler, cursorContext);
        }

        @Override
        public Map<String, Value> indexConfig() {
            return delegate.indexConfig();
        }
    }
}
