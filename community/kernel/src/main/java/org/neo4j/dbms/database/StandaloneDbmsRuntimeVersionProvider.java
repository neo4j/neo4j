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
package org.neo4j.dbms.database;

import static org.neo4j.dbms.database.SystemGraphComponent.VERSION_LABEL;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

import java.util.List;
import org.neo4j.dbms.DbmsRuntimeVersionProvider;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.util.VisibleForTesting;

/**
 * A version of {@link DbmsRuntimeVersionProvider} for standalone editions.
 */
public class StandaloneDbmsRuntimeVersionProvider
        implements TransactionEventListener<Object>, DbmsRuntimeVersionProvider {

    protected final DbmsRuntimeSystemGraphComponent component;
    private final DatabaseContextProvider<?> databaseContextProvider;
    private volatile DbmsRuntimeVersion currentVersion;

    public StandaloneDbmsRuntimeVersionProvider(
            DatabaseContextProvider<?> databaseContextProvider, DbmsRuntimeSystemGraphComponent component) {
        this.databaseContextProvider = databaseContextProvider;
        this.component = component;
    }

    @Override
    public Object beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService)
            throws Exception {
        // not interested in this event
        return null;
    }

    @Override
    public void afterCommit(TransactionData transactionData, Object state, GraphDatabaseService databaseService) {
        // no check is needed if we are at the latest version, because downgrade is not supported
        if (transactionData == null || getVersion().isCurrent(component.config)) {
            return;
        }

        List<Long> nodesWithChangedProperties = Iterables.stream(transactionData.assignedNodeProperties())
                .map(nodePropertyEntry -> nodePropertyEntry.entity().getId())
                .toList();

        var systemDatabase = getSystemDb();
        try (var tx = systemDatabase.beginTx()) {
            nodesWithChangedProperties.stream()
                    .map(tx::getNodeById)
                    .filter(node -> node.hasLabel(VERSION_LABEL)
                            && node.hasProperty(component.componentName().name()))
                    .map(dbmRuntime -> (int)
                            dbmRuntime.getProperty(component.componentName().name()))
                    .map(DbmsRuntimeVersion::fromVersionNumber)
                    .forEach(this::setVersion);
        }
    }

    @Override
    public void afterRollback(TransactionData data, Object state, GraphDatabaseService databaseService) {
        // not interested in this event
    }

    private void fetchStateFromSystemDatabase() {
        var systemDatabase = getSystemDb();
        currentVersion = component.fetchStateFromSystemDatabase(systemDatabase);
    }

    private GraphDatabaseService getSystemDb() {
        return databaseContextProvider
                .getDatabaseContext(NAMED_SYSTEM_DATABASE_ID)
                .orElseThrow(() -> new RuntimeException("Failed to get System Database"))
                .databaseFacade();
    }

    @Override
    public DbmsRuntimeVersion getVersion() {
        if (currentVersion == null) {
            synchronized (this) {
                if (currentVersion == null) {
                    fetchStateFromSystemDatabase();
                }
            }
        }

        return currentVersion;
    }

    /**
     * This must be used only by children and tests!!!
     */
    @VisibleForTesting
    void setVersion(DbmsRuntimeVersion newVersion) {
        currentVersion = newVersion;
    }
}
