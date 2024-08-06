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

import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.dbms.systemgraph.CommunityTopologyGraphDbmsModel;
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

public final class SystemGraphReadOnlyDatabaseLookupFactory implements ReadOnlyDatabases.LookupFactory {
    private final SystemDatabaseProvider systemDatabaseProvider;
    private final InternalLog log;

    private volatile SystemGraphLookup previousLookup;

    public SystemGraphReadOnlyDatabaseLookupFactory(
            SystemDatabaseProvider systemDatabaseProvider, InternalLogProvider logProvider) {
        this.systemDatabaseProvider = systemDatabaseProvider;
        this.previousLookup = SystemGraphLookup.ALWAYS_READONLY;
        this.log = logProvider.getLog(getClass());
    }

    @Override
    public ReadOnlyDatabases.Lookup lookupReadOnlyDatabases() {
        var previous = previousLookup;
        var next = previous;

        try {
            next = systemDatabaseProvider
                    .queryIfAvailable(this::lookupReadOnlyDatabases)
                    .map(set -> new SystemGraphLookup(false, set))
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

    private Set<DatabaseId> lookupReadOnlyDatabases(Transaction tx) {
        var model = new CommunityTopologyGraphDbmsModel(tx);
        var databaseAccess = model.getAllDatabaseAccess();
        return databaseAccess.entrySet().stream()
                .filter(e -> e.getValue() == TopologyGraphDbmsModel.DatabaseAccess.READ_ONLY)
                .map(e -> e.getKey().databaseId())
                .collect(Collectors.toUnmodifiableSet());
    }

    private record SystemGraphLookup(boolean alwaysReadOnly, Set<DatabaseId> databaseIds)
            implements ReadOnlyDatabases.Lookup {
        private static final SystemGraphLookup ALWAYS_READONLY = new SystemGraphLookup(true, Set.of());

        @Override
        public boolean databaseIsReadOnly(DatabaseId databaseId) {
            return alwaysReadOnly || databaseIds.contains(databaseId);
        }

        @Override
        public Source source() {
            return Source.SYSTEM_GRAPH;
        }
    }
}
