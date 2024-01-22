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
package org.neo4j.kernel.internal.event;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class DatabaseTransactionEventListeners extends LifecycleAdapter {
    private final GraphDatabaseFacade databaseFacade;
    private final GlobalTransactionEventListeners globalTransactionEventListeners;
    private final String databaseName;
    private final Set<TransactionEventListener<?>> listeners = ConcurrentHashMap.newKeySet();

    public DatabaseTransactionEventListeners(
            GraphDatabaseFacade databaseFacade,
            GlobalTransactionEventListeners globalTransactionEventListeners,
            NamedDatabaseId namedDatabaseId) {
        this.databaseFacade = databaseFacade;
        this.globalTransactionEventListeners = globalTransactionEventListeners;
        this.databaseName = namedDatabaseId.name();
    }

    public void registerTransactionEventListener(TransactionEventListener<?> listener) {
        globalTransactionEventListeners.registerTransactionEventListener(databaseName, listener);
        listeners.add(listener);
    }

    public void unregisterTransactionEventListener(TransactionEventListener<?> listener) {
        listeners.remove(listener);
        globalTransactionEventListeners.unregisterTransactionEventListener(databaseName, listener);
    }

    @Override
    public void shutdown() {
        for (TransactionEventListener<?> listener : listeners) {
            globalTransactionEventListeners.unregisterTransactionEventListener(databaseName, listener);
        }
    }

    public Collection<TransactionEventListener<?>> getCurrentRegisteredTransactionEventListeners() {
        return globalTransactionEventListeners.getDatabaseTransactionEventListeners(databaseName);
    }

    public GraphDatabaseFacade getDatabaseFacade() {
        return databaseFacade;
    }
}
