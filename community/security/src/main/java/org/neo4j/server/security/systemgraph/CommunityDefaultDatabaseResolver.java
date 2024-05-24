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
package org.neo4j.server.security.systemgraph;

import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_DEFAULT_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.kernel.database.DefaultDatabaseResolver;

public class CommunityDefaultDatabaseResolver extends TransactionEventListenerAdapter<Object>
        implements DefaultDatabaseResolver {
    private final AtomicReference<String> cachedDefaultDatabase = new AtomicReference<>();

    protected final Supplier<String> defaultDbSupplier;
    protected final SystemDatabaseProvider systemDbProvider;

    public CommunityDefaultDatabaseResolver(Config config, SystemDatabaseProvider systemDbProvider) {
        this(() -> config.get(GraphDatabaseSettings.initial_default_database), systemDbProvider);
    }

    protected CommunityDefaultDatabaseResolver(
            Supplier<String> defaultDbSupplier, SystemDatabaseProvider systemDbProvider) {
        this.defaultDbSupplier = defaultDbSupplier;
        this.systemDbProvider = systemDbProvider;
    }

    @Override
    public String defaultDatabase(String username) {
        var defaultDatabase = cachedDefaultDatabase.get();
        if (defaultDatabase == null) {
            defaultDatabase = systemDbProvider
                    .query(CommunityDefaultDatabaseResolver::resolveDefaultDatabase)
                    .orElseGet(defaultDbSupplier);
            cachedDefaultDatabase.set(defaultDatabase);
        }
        return defaultDatabase;
    }

    @Override
    public void clearCache() {
        cachedDefaultDatabase.set(null);
    }

    @Override
    public void afterCommit(TransactionData data, Object state, GraphDatabaseService databaseService) {
        clearCache();
    }

    private static Optional<String> resolveDefaultDatabase(Transaction tx) {
        return Optional.ofNullable(tx.findNode(DATABASE_LABEL, DATABASE_DEFAULT_PROPERTY, true))
                .flatMap(defaultDatabaseNode ->
                        Optional.ofNullable((String) defaultDatabaseNode.getProperty(DATABASE_NAME_PROPERTY, null)));
    }
}
