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

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.dbms.systemgraph.CommunityTopologyGraphDbmsModel;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

public final class SystemGraphReadOnlyDatabaseLookupFactory implements ReadOnlyDatabases.LookupFactory {
    private final DatabaseContextProvider<?> databaseContextProvider;
    private final InternalLog log;

    private volatile SystemGraphLookup previousLookup;

    public SystemGraphReadOnlyDatabaseLookupFactory(
            DatabaseContextProvider<?> databaseContextProvider, InternalLogProvider logProvider) {
        this.databaseContextProvider = databaseContextProvider;
        this.previousLookup = SystemGraphLookup.ALWAYS_READONLY;
        this.log = logProvider.getLog(getClass());
    }

    private Optional<GraphDatabaseAPI> systemDatabase() {
        var systemDb = databaseContextProvider.getDatabaseContext(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID);
        var started = systemDb.map(db -> db.databaseFacade().isAvailable()).orElse(false);

        if (started) {
            return systemDb.map(DatabaseContext::databaseFacade);
        }
        return Optional.empty();
    }

    @Override
    public ReadOnlyDatabases.Lookup lookupReadOnlyDatabases() {
        var previous = previousLookup;
        var next = previous;

        try {
            next = systemDatabase()
                    .map(this::lookupReadOnlyDatabases)
                    .map(dbs -> new SystemGraphLookup(dbs, false))
                    .orElse(previous);
        } catch (Exception e) {
            log.warn(
                    "Unable to lookup readonly databases from the system database due to error!"
                            + " Using previous lookup %s.%nUnderlying error: %s",
                    previous, e.getMessage());
        }

        this.previousLookup = next;
        return next;
    }

    private Set<DatabaseId> lookupReadOnlyDatabases(GraphDatabaseAPI db) {
        try (var tx = db.beginTx()) {
            var model = new CommunityTopologyGraphDbmsModel(tx);
            var databaseAccess = model.getAllDatabaseAccess();
            return databaseAccess.entrySet().stream()
                    .filter(e -> e.getValue() == TopologyGraphDbmsModel.DatabaseAccess.READ_ONLY)
                    .map(e -> e.getKey().databaseId())
                    .collect(Collectors.toUnmodifiableSet());
        }
    }

    private static class SystemGraphLookup implements ReadOnlyDatabases.Lookup {
        static final SystemGraphLookup ALWAYS_READONLY = new SystemGraphLookup(Set.of(), true);

        private final Set<DatabaseId> lookup;
        private final boolean alwaysReadOnly;

        SystemGraphLookup(Set<DatabaseId> lookup, boolean alwaysReadOnly) {
            this.lookup = lookup;
            this.alwaysReadOnly = alwaysReadOnly;
        }

        @Override
        public boolean databaseIsReadOnly(DatabaseId databaseId) {
            return alwaysReadOnly || lookup.contains(databaseId);
        }

        @Override
        public Source source() {
            return Source.SYSTEM_GRAPH;
        }

        @Override
        public String toString() {
            return "SystemGraphLookup{" + "readOnlyDatabases=" + lookup + ", alwaysReadOnly=" + alwaysReadOnly + '}';
        }
    }
}
