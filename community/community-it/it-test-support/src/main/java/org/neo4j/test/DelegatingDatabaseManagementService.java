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
package org.neo4j.test;

import java.util.List;
import org.neo4j.dbms.api.DatabaseAliasExistsException;
import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.event.TransactionEventListener;

public class DelegatingDatabaseManagementService implements DatabaseManagementService {
    final DatabaseManagementService delegate;

    public DelegatingDatabaseManagementService(DatabaseManagementService delegate) {
        this.delegate = delegate;
    }

    @Override
    public GraphDatabaseService database(String databaseName) throws DatabaseNotFoundException {
        return delegate.database(databaseName);
    }

    @Override
    public void createDatabase(String databaseName, Configuration databaseSpecificSettings)
            throws DatabaseExistsException {
        delegate.createDatabase(databaseName, databaseSpecificSettings);
    }

    @Override
    public void dropDatabase(String databaseName) throws DatabaseNotFoundException, DatabaseAliasExistsException {
        delegate.dropDatabase(databaseName);
    }

    @Override
    public void startDatabase(String databaseName) throws DatabaseNotFoundException {
        delegate.startDatabase(databaseName);
    }

    @Override
    public void shutdownDatabase(String databaseName) throws DatabaseNotFoundException {
        delegate.shutdownDatabase(databaseName);
    }

    @Override
    public List<String> listDatabases() {
        return delegate.listDatabases();
    }

    @Override
    public void registerDatabaseEventListener(DatabaseEventListener listener) {
        delegate.registerDatabaseEventListener(listener);
    }

    @Override
    public void unregisterDatabaseEventListener(DatabaseEventListener listener) {
        delegate.unregisterDatabaseEventListener(listener);
    }

    @Override
    public void registerTransactionEventListener(String databaseName, TransactionEventListener<?> listener) {
        delegate.registerTransactionEventListener(databaseName, listener);
    }

    @Override
    public void unregisterTransactionEventListener(String databaseName, TransactionEventListener<?> listener) {
        delegate.unregisterTransactionEventListener(databaseName, listener);
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    public static class AutoCloseable extends DelegatingDatabaseManagementService implements java.lang.AutoCloseable {
        public AutoCloseable(DatabaseManagementService delegate) {
            super(delegate);
        }

        @Override
        public void close() {
            shutdown();
        }
    }
}
