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
package org.neo4j.kernel.impl.query;

import org.neo4j.graphdb.QueryStatistics;

/**
 * A QuerySubscriber that only counts completed records.
 *
 * NOTE: This does not consume records with something like Blackhole, so should not be used for CPU profiling.
 */
public class NonRecordingQuerySubscriber extends ProbingQuerySubscriber implements QuerySubscriber {
    private Throwable throwable;
    private QueryStatistics statistics;
    private long count;

    public NonRecordingQuerySubscriber(QuerySubscriberProbe probe) {
        super(probe);
    }

    public NonRecordingQuerySubscriber() {
        super();
    }

    @Override
    public void onRecordCompleted() {
        super.onRecordCompleted();
        count++;
    }

    @Override
    public void onError(Throwable throwable) {
        super.onError(throwable);
        if (this.throwable == null) {
            this.throwable = throwable;
        }
    }

    @Override
    public void onResultCompleted(QueryStatistics statistics) {
        super.onResultCompleted(statistics);
        this.statistics = statistics;
    }

    public void assertNoErrors() throws Throwable {
        if (throwable != null) {
            throw throwable;
        }
    }

    public long recordCount() {
        return count;
    }

    public QueryStatistics queryStatistics() {
        return statistics;
    }
}
