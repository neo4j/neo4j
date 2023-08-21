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
package org.neo4j.kernel.monitoring;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.event.DatabaseEventListenerInternal;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.InternalLog;

/**
 * Handle the collection of database event listeners, and fire events as needed.
 */
public class DatabaseEventListeners {
    private final List<DatabaseEventListener> databaseEventListeners = new CopyOnWriteArrayList<>();
    private final InternalLog log;

    public DatabaseEventListeners(InternalLog log) {
        this.log = log;
    }

    public void registerDatabaseEventListener(DatabaseEventListener listener) {
        addListener(listener, databaseEventListeners);
    }

    public void unregisterDatabaseEventListener(DatabaseEventListener listener) {
        removeListener(listener, databaseEventListeners);
    }

    private static <T> void addListener(T listener, List<T> listeners) {
        if (listeners.contains(listener)) {
            return;
        }
        listeners.add(listener);
    }

    private static <T> void removeListener(T listener, List<T> listeners) {
        if (!listeners.remove(listener)) {
            throw new IllegalStateException("Database listener `" + listener + "` is not registered.");
        }
    }

    public void databaseStart(NamedDatabaseId databaseId) {
        var event = new DefaultDatabaseEvent(databaseId);
        notifyEventListeners(handler -> handler.databaseStart(event), databaseEventListeners);
    }

    public void databaseShutdown(NamedDatabaseId databaseId) {
        var event = new DefaultDatabaseEvent(databaseId);
        notifyEventListeners(handler -> handler.databaseShutdown(event), databaseEventListeners);
    }

    public void databaseCreate(NamedDatabaseId databaseId) {
        var event = new DefaultDatabaseEvent(databaseId);
        notifyEventListeners(handler -> handler.databaseCreate(event), databaseEventListeners);
    }

    public void databaseDrop(NamedDatabaseId databaseId) {
        var event = new DefaultDatabaseEvent(databaseId);
        notifyEventListeners(handler -> handler.databaseDrop(event), databaseEventListeners);
    }

    void databasePanic(NamedDatabaseId databaseId, Throwable causeOfPanic) {
        var event = new ExceptionalDatabaseEvent(databaseId, causeOfPanic);
        notifyEventListeners(handler -> handler.databasePanic(event), databaseEventListeners);
    }

    void databaseOutOfDiskSpace(NamedDatabaseId databaseId, Throwable cause) {
        var event = new ExceptionalDatabaseEvent(databaseId, cause);
        notifyEventListeners(
                handler -> {
                    if (handler instanceof DatabaseEventListenerInternal internal) {
                        internal.databaseOutOfDiskSpace(event);
                    }
                },
                databaseEventListeners);
    }

    private <T> void notifyEventListeners(Consumer<T> consumer, List<T> listeners) {
        for (var listener : listeners) {
            try {
                consumer.accept(listener);
            } catch (Throwable e) {
                log.error("Error while handling database event by listener: " + listener, e);
            }
        }
    }
}
