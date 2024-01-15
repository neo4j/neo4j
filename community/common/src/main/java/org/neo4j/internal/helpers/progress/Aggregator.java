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
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

final class Aggregator {
    private final Map<ProgressListener, State> states = new HashMap<>();
    private final Indicator indicator;

    @SuppressWarnings("unused" /*accessed through updater*/)
    private volatile long progress;

    private volatile int last;

    private static final AtomicLongFieldUpdater<Aggregator> PROGRESS_UPDATER = newUpdater(Aggregator.class, "progress");
    private volatile long totalCount;

    Aggregator(Indicator indicator) {
        this.indicator = indicator;
    }

    synchronized void add(ProgressListener progress, long totalCount) {
        states.put(progress, State.INIT);
        this.totalCount += totalCount;
    }

    synchronized ProgressMonitorFactory.Completer initialize() {
        indicator.startProcess(totalCount);
        if (states.isEmpty()) {
            indicator.progress(0, indicator.reportResolution());
        }

        List<ProgressListener> progressesToClose = new ArrayList<>(states.keySet());
        return () -> progressesToClose.forEach(ProgressListener::close);
    }

    void update(long delta) {
        if (delta > 0) {
            long progress = min(totalCount, PROGRESS_UPDATER.addAndGet(this, delta));
            if (progress > 0) {
                int current = (int) ((progress * indicator.reportResolution()) / totalCount);
                if (current > last) {
                    updateTo(current);
                }
            }
        }
    }

    private synchronized void updateTo(int to) {
        if (to > last) {
            indicator.progress(last, to);
            last = to;
        }
    }

    void updateRemaining() {
        updateTo(indicator.reportResolution());
    }

    synchronized void start(ProgressListener.MultiPartProgressListener part) {
        states.put(part, State.LIVE);
    }

    synchronized void complete(ProgressListener.MultiPartProgressListener part) {
        if (states.remove(part) != null) {
            if (states.isEmpty()) {
                updateRemaining();
            }
        }
    }

    synchronized void signalFailure(Throwable e) {
        indicator.failure(e);
    }

    void done() {
        states.keySet().forEach(ProgressListener::close);
    }

    void mark(char mark) {
        indicator.mark(mark);
    }

    enum State {
        INIT,
        LIVE
    }
}
