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

import java.util.concurrent.atomic.AtomicLong;

/**
 * A Progress object is an object through which a process can report its progress.
 * <p>
 * Progress objects are not thread safe, and are to be used by a single thread only. Each Progress object from a {@link
 * ProgressMonitorFactory.MultiPartBuilder} can be used from different threads.
 */
public interface ProgressListener extends AutoCloseable {
    void add(long progress);

    void mark(char mark);

    @Override
    void close();

    void failed(Throwable e);

    ProgressListener threadLocalReporter(int threshold);

    default ProgressListener threadLocalReporter() {
        return threadLocalReporter(1_000);
    }

    class Adapter implements ProgressListener {
        @Override
        public void add(long progress) {}

        @Override
        public void mark(char mark) {}

        @Override
        public void close() {}

        @Override
        public void failed(Throwable e) {}

        @Override
        public ProgressListener threadLocalReporter(int threshold) {
            return new ThreadLocalReporter(threshold, this);
        }
    }

    ProgressListener NONE = new Adapter();

    class ThreadLocalReporter extends Adapter {
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
        public ProgressListener threadLocalReporter(int threshold) {
            return new ThreadLocalReporter(threshold, this);
        }
    }

    class SinglePartProgressListener extends Adapter {
        private final Aggregator aggregator;

        SinglePartProgressListener(
                Indicator indicator, long totalCount, ProgressMonitorFactory.IndicatorListener listener) {
            this.aggregator = new Aggregator(indicator, listener);
            aggregator.add(new Adapter() {}, totalCount);
            aggregator.initialize();
        }

        @Override
        public void add(long progress) {
            aggregator.update(progress);
        }

        @Override
        public void mark(char mark) {
            aggregator.mark(mark);
        }

        @Override
        public void close() {
            aggregator.updateRemaining();
            aggregator.done();
        }

        @Override
        public void failed(Throwable e) {
            aggregator.signalFailure(e);
        }
    }

    final class MultiPartProgressListener extends Adapter {
        public final String part;
        public final long totalCount;

        private final Aggregator aggregator;
        private final AtomicLong progress = new AtomicLong();

        MultiPartProgressListener(Aggregator aggregator, String part, long totalCount) {
            this.aggregator = aggregator;
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
        public void mark(char mark) {
            aggregator.mark(mark);
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

        @Override
        public void failed(Throwable e) {
            aggregator.signalFailure(e);
        }
    }
}
