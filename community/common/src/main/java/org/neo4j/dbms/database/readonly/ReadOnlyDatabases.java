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

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * Dbms global component for checking whether a given database is read only.
 */
public interface ReadOnlyDatabases {

    /**
     * Checks whether the database with the given {@code databaseId} is configured to be read-only.
     *
     * @param databaseId the identity of the database to check.
     * @return {@code true} if the database is read-only, otherwise {@code false}.
     */
    boolean isReadOnly(DatabaseId databaseId);

    /**
     * Checks whether the database with the given {@code databaseId} is configured to be read-only locally on this server.
     *
     * @param databaseId the identity of the database to check.
     * @return {@code true} if the database is read-only on this server, otherwise {@code false}.
     */
    boolean isReadOnlyLocally(DatabaseId databaseId);

    /**
     * Instantiates and returns a {@link DatabaseReadOnlyChecker} which is primed to check read-only state of the database with
     * the given {@code namedDatabaseId}.
     *
     * @param namedDatabaseId the identity of the database to instantiate a {@link DatabaseReadOnlyChecker} for.
     * @return a new {@link DatabaseReadOnlyChecker} for the given database.
     */
    DatabaseReadOnlyChecker forDatabase(NamedDatabaseId namedDatabaseId);

    /**
     * Refresh the ReadOnlyChecker's internal cache. I.e. in the event that the config or System database is updated, or a
     * new {@link DatabaseReadOnlyChecker} is created.
     */
    void refresh();

    /**
     * @return a numeric value which increases monotonically with each call to {@link #refresh()}. Used by {@link DatabaseReadOnlyChecker} for caching.
     */
    long updateId();

    /**
     * Objects implementing this interface create {@link Lookup}s: immutable snapshots of the logical set of read only databases
     */
    @FunctionalInterface
    interface LookupFactory {
        Lookup lookupReadOnlyDatabases();
    }

    /**
     * Objects implementing this interface are immutable snapshots of a logical set of read only databases. The interface is
     * analogous to a {@code Predicate<NamedDatabaseId>} or a {@code Set#contains} method reference over an immutable set.
     */
    interface Lookup {
        boolean databaseIsReadOnly(DatabaseId databaseId);

        Source source();

        enum Source {
            CONFIG,
            SYSTEM_GRAPH
        }
    }
}
