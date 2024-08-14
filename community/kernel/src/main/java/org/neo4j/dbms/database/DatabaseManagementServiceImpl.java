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

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.logging.InternalLog;

public class DatabaseManagementServiceImpl implements DatabaseManagementService {
    private final DatabaseContextProvider<?> databaseContextProvider;
    private final Lifecycle globalLife;
    private final DatabaseEventListeners databaseEventListeners;
    private final GlobalTransactionEventListeners transactionEventListeners;
    private final InternalLog log;
    private final Config globalConfig;

    public DatabaseManagementServiceImpl(
            DatabaseContextProvider<?> databaseContextProvider,
            Lifecycle globalLife,
            DatabaseEventListeners databaseEventListeners,
            GlobalTransactionEventListeners transactionEventListeners,
            InternalLog log,
            Config globalConfig) {
        this.databaseContextProvider = databaseContextProvider;
        this.globalLife = globalLife;
        this.databaseEventListeners = databaseEventListeners;
        this.transactionEventListeners = transactionEventListeners;
        this.log = log;
        this.globalConfig = globalConfig;
    }

    @Override
    public GraphDatabaseService database(String name) throws DatabaseNotFoundException {
        return databaseContextProvider
                .databaseIdRepository()
                .getByName(name)
                .flatMap(databaseContextProvider::getDatabaseContext)
                .map(DatabaseContext::databaseFacade)
                .orElseThrow(() -> new DatabaseNotFoundException(name));
    }

    @Override
    public void createDatabase(String name) throws DatabaseExistsException {
        systemDatabaseExecute("CREATE DATABASE $name", Map.of("name", name));
    }

    @Override
    public void createDatabase(String name, Configuration databaseSpecificSettings) {
        String storeFormat = getStoreFormat(databaseSpecificSettings);
        systemDatabaseExecute(
                "CREATE DATABASE $name OPTIONS {storeFormat: $storeFormat}",
                Map.of("name", name, "storeFormat", storeFormat));
    }

    private String getStoreFormat(Configuration databaseSpecificSettings) {
        String dbSpecificStoreFormat = null;
        if (!(databaseSpecificSettings instanceof Config)
                || ((Config) databaseSpecificSettings).isExplicitlySet(db_format)) {
            dbSpecificStoreFormat = databaseSpecificSettings.get(db_format);
        }
        return dbSpecificStoreFormat != null ? dbSpecificStoreFormat : globalConfig.get(db_format);
    }

    @Override
    public void dropDatabase(String name) {
        systemDatabaseExecute("DROP DATABASE $name", Map.of("name", name));
    }

    @Override
    public void startDatabase(String name) {
        systemDatabaseExecute("START DATABASE $name", Map.of("name", name));
    }

    @Override
    public void shutdownDatabase(String name) {
        systemDatabaseExecute("STOP DATABASE $name", Map.of("name", name));
    }

    @Override
    public List<String> listDatabases() {
        return databaseContextProvider.registeredDatabases().keySet().stream()
                .map(NamedDatabaseId::name)
                .sorted()
                .collect(Collectors.toList());
    }

    @Override
    public void registerDatabaseEventListener(DatabaseEventListener listener) {
        databaseEventListeners.registerDatabaseEventListener(listener);
    }

    @Override
    public void unregisterDatabaseEventListener(DatabaseEventListener listener) {
        databaseEventListeners.unregisterDatabaseEventListener(listener);
    }

    @Override
    public void registerTransactionEventListener(String databaseName, TransactionEventListener<?> listener) {
        validateDatabaseName(databaseName);
        transactionEventListeners.registerTransactionEventListener(databaseName, listener);
    }

    @Override
    public void unregisterTransactionEventListener(String databaseName, TransactionEventListener<?> listener) {
        transactionEventListeners.unregisterTransactionEventListener(databaseName, listener);
    }

    @Override
    public void shutdown() {
        try {
            log.info("Shutdown started");
            globalLife.shutdown();
        } catch (Exception throwable) {
            String message = "Shutdown failed";
            log.error(message, throwable);
            throw new RuntimeException(message, throwable);
        }
    }

    private void systemDatabaseExecute(String query, Map<String, Object> parameters) {
        try {
            GraphDatabaseAPI database = (GraphDatabaseAPI) database(SYSTEM_DATABASE_NAME);
            try (InternalTransaction transaction =
                    database.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
                transaction.execute(query, parameters);
                transaction.commit();
            }
        } catch (QueryExecutionException e) {
            throw new DatabaseManagementException(e);
        }
    }

    private static void validateDatabaseName(String databaseName) {
        if (SYSTEM_DATABASE_NAME.equals(databaseName)) {
            throw new IllegalArgumentException(
                    "Registration of transaction event listeners on " + SYSTEM_DATABASE_NAME + " is not supported.");
        }
    }
}
