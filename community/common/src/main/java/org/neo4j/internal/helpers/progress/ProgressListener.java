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
package org.neo4j.internal.helpers.progress;

import static java.lang.Long.min;

import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory.IndicatorListener;

/**
 * A Progress object is an object through which a process can report its progress.
 * <p>
 * Progress objects are thread safe, and can be used by multiple threads. For best performance tho,
 * each thread advancing progress should do so in its own {@link #threadLocalReporter()}.
 */
public interface ProgressListener extends AutoCloseable {
    /**
     * @param progress amount of progress to advance this listener. It's a delta value such that the total
     * progress is the sum of all these calls.
     */
    void add(long progress);

    /**
     * Place a mark in this progress output such that the next progress "dot" being written will be
     * this mark character instead of the usual dot.
     * @param mark the character replacing the dot for the next progress "dot" to write.
     */
    void mark(char mark);

    /**
     * Advances the progress to the end and closes any resources attached to it.
     */
    @Override
    void close();

    /**
     * Lets the given error be printed into the progress output.
     * @param e the error to print.
     */
    void failed(Throwable e);

    /**
     * @param threshold the interval at which progress is reported to the parent listener.
     * @return a {@link ProgressListener} which should only be used by the same thread calling this method.
     * It works just as the "parent" listener, but has a local counter and only reports to the parent listener
     * at a certain threshold value, e.g. for a threshold of 1000 then at least 1000 worth of progress
     * will be gathered in the thread-local listener and reported back to parent in one call once the
     * threshold is reached, where its local value will be reset to accept more local progress.
     */
    default ProgressListener threadLocalReporter(int threshold) {
        return new ThreadLocalReporter(threshold, this);
    }

    /**
     * @return a thread-local {@link ProgressListener} with threshold 1000.
     * @see #threadLocalReporter(int)
     */
    default ProgressListener threadLocalReporter() {
        return threadLocalReporter(1_000);
    }

    /**
     * @return the resolution of this progress, i.e. how many "dots" in total it prints.
     */
    int reportResolution();

    interface WithProgressGetter extends ProgressListener {
        /**
         * @return the current progress
         */
        long getProgress();
    }

    class Adapter implements ProgressListener.WithProgressGetter {
        @Override
        public void add(long progress) {}

        @Override
        public void mark(char mark) {}

        @Override
        public void close() {}

        @Override
        public void failed(Throwable e) {}

        @Override
        public int reportResolution() {
            return 0;
        }

        @Override
        public long getProgress() {
            return 0;
        }
    }

    ProgressListener.WithProgressGetter NONE = new Adapter();

    class ThreadLocalReporter implements ProgressListener {
        private final int threshold;
        private final ProgressListener parent;
        private int localUnreportedProgress;
        private Character mark;

        ThreadLocalReporter(int threshold, ProgressListener parent) {
            this.threshold = threshold;
            this.parent = parent;
        }

        @Override
        public void add(long progress) {
            localUnreportedProgress += progress;
            if (localUnreportedProgress >= threshold) {
                reportToParent();
            }
        }

        @Override
        public void mark(char mark) {
            this.mark = mark;
        }

        @Override
        public void close() {
            reportToParent();
        }

        private void reportToParent() {
            if (mark != null) {
                parent.mark(mark);
                mark = null;
            }
            parent.add(localUnreportedProgress);
            localUnreportedProgress = 0;
        }

        @Override
        public void failed(Throwable e) {
            parent.failed(e);
        }

        @Override
        public int reportResolution() {
            return parent.reportResolution();
        }
    }

    abstract class AggregatorProgressListener implements ProgressListener {
        protected final Aggregator aggregator;

        AggregatorProgressListener(Aggregator aggregator) {
            this.aggregator = aggregator;
        }

        @Override
        public void mark(char mark) {
            aggregator.mark(mark);
        }

        @Override
        public void failed(Throwable e) {
            aggregator.signalFailure(e);
        }

        @Override
        public int reportResolution() {
            return aggregator.reportResolution();
        }
    }

    /** Used when the progress is being done by a single thread, meaning we can forego synchronization for speed
     */
    class SingleThreadedSinglePartProgressListener implements ProgressListener {
        private final Indicator indicator;
        private final long totalCount;
        private final IndicatorListener listener;
        private long progress;
        private int last;

        SingleThreadedSinglePartProgressListener(
                Indicator indicator, long totalCount, ProgressMonitorFactory.IndicatorListener listener) {
            this.indicator = indicator;
            this.totalCount = totalCount;
            this.listener = listener;
            indicator.startProcess(totalCount);
        }

        @Override
        public void add(long delta) {
            if (delta > 0) {
                progress += delta;
                long cappedProgress = min(totalCount, progress);
                if (cappedProgress > 0) {
                    int current = (int) ((cappedProgress * indicator.reportResolution()) / totalCount);
                    updateTo(current, cappedProgress);
                }
            }
        }

        private void updateTo(int current, long cappedProgress) {
            if (current > last) {
                indicator.progress(last, current);
                listener.update(cappedProgress, totalCount);
                last = current;
            }
        }

        @Override
        public void mark(char mark) {
            indicator.mark(mark);
        }

        @Override
        public void close() {
            updateTo(indicator.reportResolution(), totalCount);
        }

        @Override
        public void failed(Throwable e) {
            indicator.failure(e);
        }

        @Override
        public int reportResolution() {
            return indicator.reportResolution();
        }
    }

    class SinglePartProgressListener extends AggregatorProgressListener implements WithProgressGetter {
        SinglePartProgressListener(
                Indicator indicator, long totalCount, ProgressMonitorFactory.IndicatorListener listener) {
            super(new Aggregator(indicator, listener));
            aggregator.add(new Adapter() {}, totalCount);
            aggregator.initialize();
        }

        @Override
        public void add(long progress) {
            aggregator.update(progress);
        }

        @Override
        public long getProgress() {
            return aggregator.getProgress();
        }

        @Override
        public void close() {
            aggregator.updateRemaining();
            aggregator.done();
        }
    }

    final class MultiPartProgressListener extends AggregatorProgressListener {
        public final String part;
        public final long totalCount;

        private final AtomicLong progress = new AtomicLong();

        MultiPartProgressListener(Aggregator aggregator, String part, long totalCount) {
            super(aggregator);
            this.part = part;
            this.totalCount = totalCount;
            aggregator.start(this);
        }

        @Override
        public void add(long delta) {
            long current = progress.get();
            if (current + delta > totalCount) {
                delta = totalCount - current;
            }
            if (delta > 0) {
                progress.addAndGet(delta);
                aggregator.update(delta);
            }
        }

        @Override
        public synchronized void close() {
            long delta = totalCount - progress.get();
            if (delta > 0) {
                add(delta);
            }

            // Idempotent call
            aggregator.complete(this);
        }
    }
}
