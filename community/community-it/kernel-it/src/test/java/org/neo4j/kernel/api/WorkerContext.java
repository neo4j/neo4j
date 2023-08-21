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
package org.neo4j.kernel.api;

import static org.neo4j.io.IOUtils.closeAllUnchecked;

public class WorkerContext<T extends AutoCloseable> implements AutoCloseable {
    private final T cursor;
    private final ExecutionContext context;
    private final KernelTransaction owner;
    private final Statement statement;

    public WorkerContext(T cursor, ExecutionContext context, KernelTransaction owner, Statement statement) {
        this.cursor = cursor;
        this.context = context;
        this.owner = owner;
        this.statement = statement;
    }

    @Override
    public void close() {
        closeAllUnchecked(context, statement);
    }

    public T getCursor() {
        return cursor;
    }

    public ExecutionContext getContext() {
        return context;
    }

    public KernelTransaction getTransaction() {
        return owner;
    }

    public void complete() {
        closeAllUnchecked(cursor);
        context.complete();
    }
}
