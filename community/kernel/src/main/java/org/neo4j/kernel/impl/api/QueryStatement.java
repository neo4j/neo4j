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
package org.neo4j.kernel.impl.api;

import static java.lang.invoke.MethodHandles.lookup;
import static org.neo4j.internal.helpers.VarHandleUtils.getVarHandle;

import java.lang.invoke.VarHandle;
import java.util.Optional;
import org.neo4j.kernel.api.query.ExecutingQuery;

public abstract class QueryStatement extends CloseableResourceManager implements StatementInfo {

    private static final VarHandle EXECUTING_QUERY = getVarHandle(lookup(), "executingQuery");
    private volatile ExecutingQuery executingQuery;

    protected Optional<ExecutingQuery> executingQuery() {
        return Optional.ofNullable(executingQuery);
    }

    protected ExecutingQuery executingQueryPlain() {
        return (ExecutingQuery) EXECUTING_QUERY.get(this);
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
