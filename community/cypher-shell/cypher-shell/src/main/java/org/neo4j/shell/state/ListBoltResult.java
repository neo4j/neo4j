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
package org.neo4j.shell.state;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.neo4j.driver.Record;
import org.neo4j.driver.summary.ResultSummary;

/**
 * A fully materialized Cypher result.
 */
public class ListBoltResult implements BoltResult {

    private final List<String> keys;
    private final List<Record> records;
    private final ResultSummary summary;

    public ListBoltResult(List<Record> records, ResultSummary summary) {
        this(
                records,
                summary,
                records.isEmpty() ? Collections.emptyList() : records.get(0).keys());
    }

    public ListBoltResult(List<Record> records, ResultSummary summary, List<String> keys) {
        this.keys = keys;
        this.records = records;
        this.summary = summary;
    }

    @Override
    public List<String> getKeys() {
        return keys;
    }

    @Override
    public List<Record> getRecords() {
        return records;
    }

    @Override
    public Iterator<Record> iterate() {
        return records.iterator();
    }

    @Override
    public ResultSummary getSummary() {
        return summary;
    }
}
