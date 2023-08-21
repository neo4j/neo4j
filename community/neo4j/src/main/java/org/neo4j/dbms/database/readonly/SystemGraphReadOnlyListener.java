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
package org.neo4j.dbms.database.readonly;

import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public final class SystemGraphReadOnlyListener extends LifecycleAdapter {
    private final GlobalTransactionEventListeners txListeners;
    private final ReadOnlyDatabases readOnlyDatabases;

    private Listener listener;

    public SystemGraphReadOnlyListener(
            GlobalTransactionEventListeners txListeners, ReadOnlyDatabases readOnlyDatabases) {
        this.txListeners = txListeners;
        this.readOnlyDatabases = readOnlyDatabases;
    }

    @Override
    public void start() throws Exception {
        this.listener = new Listener(readOnlyDatabases);
        txListeners.registerTransactionEventListener(NAMED_SYSTEM_DATABASE_ID.name(), listener);
    }

    @Override
    public void stop() throws Exception {
        if (listener != null) {
            txListeners.unregisterTransactionEventListener(NAMED_SYSTEM_DATABASE_ID.name(), listener);
        }
    }

    private static class Listener extends TransactionEventListenerAdapter<Object> {
        private final ReadOnlyDatabases readOnlyDatabases;

        private Listener(ReadOnlyDatabases readOnlyDatabases) {
            this.readOnlyDatabases = readOnlyDatabases;
        }

        @Override
        public void afterCommit(TransactionData data, Object state, GraphDatabaseService databaseService) {
            readOnlyDatabases.refresh();
        }
    }
}
