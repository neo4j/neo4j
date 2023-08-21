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
import org.neo4j.values.AnyValue;

public class ChainableQuerySubscriberProbe implements QuerySubscriberProbe {
    private final QuerySubscriberProbe probe;
    private QuerySubscriberProbe next;

    public ChainableQuerySubscriberProbe(QuerySubscriberProbe probe) {
        this.probe = probe;
    }

    public ChainableQuerySubscriberProbe() {
        this.probe = null;
    }

    public ChainableQuerySubscriberProbe chain(QuerySubscriberProbe probe) {
        next = probe;
        return this;
    }

    @Override
    public void onResult(int numberOfFields) {
        if (probe != null) {
            probe.onResult(numberOfFields);
        }
        if (next != null) {
            next.onResult(numberOfFields);
        }
    }

    @Override
    public void onRecord() {
        if (probe != null) {
            probe.onRecord();
        }
        if (next != null) {
            next.onRecord();
        }
    }

    @Override
    public void onField(int offset, AnyValue value) {
        if (probe != null) {
            probe.onField(offset, value);
        }
        if (next != null) {
            next.onField(offset, value);
        }
    }

    @Override
    public void onRecordCompleted() {
        if (probe != null) {
            probe.onRecordCompleted();
        }
        if (next != null) {
            next.onRecordCompleted();
        }
    }

    @Override
    public void onError(Throwable throwable) {
        if (probe != null) {
            probe.onError(throwable);
        }
        if (next != null) {
            next.onError(throwable);
        }
    }

    @Override
    public void onResultCompleted(QueryStatistics statistics) {
        if (probe != null) {
            probe.onResultCompleted(statistics);
        }
        if (next != null) {
            next.onResultCompleted(statistics);
        }
    }
}
