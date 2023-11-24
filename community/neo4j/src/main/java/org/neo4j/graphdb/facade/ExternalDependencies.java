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
package org.neo4j.graphdb.facade;

import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.monitoring.Monitors;

public interface ExternalDependencies {
    /**
     * Allowed to be null. Null means that no external {@link Monitors} was created,
     * let the database create its own monitors instance.
     */
    Monitors monitors();

    DependencyResolver dependencies();

    InternalLogProvider userLogProvider();

    Iterable<ExtensionFactory<?>> extensions();

    /**
     * Configured default database event listeners
     * @return configured default listeners or empty iterable.
     */
    Iterable<DatabaseEventListener> databaseEventListeners();
}
