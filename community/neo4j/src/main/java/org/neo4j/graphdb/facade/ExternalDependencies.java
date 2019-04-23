/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb.facade;

import java.util.Map;

import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.DeferredExecutor;
import org.neo4j.scheduler.Group;

public interface ExternalDependencies
{
    /**
     * Allowed to be null. Null means that no external {@link Monitors} was created,
     * let the database create its own monitors instance.
     */
    Monitors monitors();

    DependencyResolver dependencies();

    LogProvider userLogProvider();

    Iterable<ExtensionFactory<?>> extensions();

    Map<String,URLAccessRule> urlAccessRules();

    Iterable<QueryEngineProvider> executionEngines();

    /**
     * Collection of command executors to start running once the db is started
     */
    Iterable<Pair<DeferredExecutor,Group>> deferredExecutors();
}
