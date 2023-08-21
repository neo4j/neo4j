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

import java.util.Iterator;
import java.util.List;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.summary.ResultSummary;

/**
 * Wrapper around {@link Result}. Might or might not be materialized.
 */
public class StatementBoltResult implements BoltResult {

    private final Result result;

    public StatementBoltResult(Result result) {
        this.result = result;
    }

    @Override
    public List<String> getKeys() {
        return result.keys();
    }

    @Override
    public List<Record> getRecords() {
        return result.list();
    }

    @Override
    public Iterator<Record> iterate() {
        return result;
    }

    @Override
    public ResultSummary getSummary() {
        return result.consume();
    }
}
