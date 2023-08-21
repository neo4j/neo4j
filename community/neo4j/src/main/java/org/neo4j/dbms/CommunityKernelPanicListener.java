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
package org.neo4j.dbms;

import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.kernel.monitoring.ExceptionalDatabaseEvent;

public class CommunityKernelPanicListener extends DatabaseEventListenerAdapter implements Lifecycle {

    private final DatabaseEventListeners databaseEventListeners;
    private final DatabaseContextProvider<StandaloneDatabaseContext> databaseContextProvider;

    public CommunityKernelPanicListener(
            DatabaseEventListeners databaseEventListeners,
            DatabaseContextProvider<StandaloneDatabaseContext> databaseContextProvider) {
        this.databaseEventListeners = databaseEventListeners;
        this.databaseContextProvider = databaseContextProvider;
    }

    @Override
    public void databasePanic(DatabaseEventContext eventContext) {
        databaseContextProvider
                .getDatabaseContext(eventContext.getDatabaseName())
                .ifPresent(context -> context.fail((getPanicCause(eventContext))));
    }

    private static Throwable getPanicCause(DatabaseEventContext eventContext) {
        var panic = (ExceptionalDatabaseEvent) eventContext;
        return Exceptions.findCauseOrSuppressed(
                        panic.getCause(), throwable -> throwable != null && throwable.getCause() == null)
                .orElse(panic.getCause());
    }

    @Override
    public void init() {
        databaseEventListeners.registerDatabaseEventListener(this);
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public void shutdown() {
        databaseEventListeners.unregisterDatabaseEventListener(this);
    }
}
