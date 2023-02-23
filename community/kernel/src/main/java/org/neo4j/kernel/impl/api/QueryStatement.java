/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api;

import java.util.Optional;
import org.neo4j.kernel.api.query.ExecutingQuery;

public abstract class QueryStatement extends CloseableResourceManager implements StatementInfo {
    private volatile ExecutingQuery executingQuery;

    protected Optional<ExecutingQuery> executingQuery() {
        return Optional.ofNullable(executingQuery);
    }

    void clearQueryExecution() {
        this.executingQuery = null;
    }

    void startQueryExecution(ExecutingQuery executingQuery) {
        executingQuery.setPreviousQuery(this.executingQuery);
        this.executingQuery = executingQuery;
    }

    void stopQueryExecution(ExecutingQuery executingQuery) {
        this.executingQuery = executingQuery.getPreviousQuery();
    }
}
