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

public class DelegatingQuerySubscriber implements QuerySubscriber {
    private final QuerySubscriber querySubscriber;

    public DelegatingQuerySubscriber(QuerySubscriber querySubscriber) {
        this.querySubscriber = querySubscriber;
    }

    @Override
    public void onResult(int numberOfFields) throws Exception {
        querySubscriber.onResult(numberOfFields);
    }

    @Override
    public void onRecord() throws Exception {
        querySubscriber.onRecord();
    }

    @Override
    public void onField(int offset, AnyValue value) throws Exception {
        querySubscriber.onField(offset, value);
    }

    @Override
    public void onRecordCompleted() throws Exception {
        querySubscriber.onRecordCompleted();
    }

    @Override
    public void onError(Throwable throwable) throws Exception {
        querySubscriber.onError(throwable);
    }

    @Override
    public void onResultCompleted(QueryStatistics statistics) {
        querySubscriber.onResultCompleted(statistics);
    }
}
