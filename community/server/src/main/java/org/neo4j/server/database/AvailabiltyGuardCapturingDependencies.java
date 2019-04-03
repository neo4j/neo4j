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
package org.neo4j.server.database;

import java.util.Map;

import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.availability.AvailabilityGuardInstaller;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.DeferredExecutor;
import org.neo4j.scheduler.Group;

final class AvailabiltyGuardCapturingDependencies implements GraphDatabaseFacadeFactory.Dependencies
{
    private final AvailabilityGuardInstaller guardInstaller;
    private final GraphDatabaseFacadeFactory.Dependencies wrapped;

    AvailabiltyGuardCapturingDependencies( AvailabilityGuardInstaller guardInstaller, GraphDatabaseFacadeFactory.Dependencies wrapped )
    {
        this.guardInstaller = guardInstaller;
        this.wrapped = wrapped;
    }

    @Override
    public Monitors monitors()
    {
        return wrapped.monitors();
    }

    @Override
    public LogProvider userLogProvider()
    {
        return wrapped.userLogProvider();
    }

    @Override
    public Iterable<Class<?>> settingsClasses()
    {
        return wrapped.settingsClasses();
    }

    @Override
    public Iterable<KernelExtensionFactory<?>> kernelExtensions()
    {
        return wrapped.kernelExtensions();
    }

    @Override
    public Map<String,URLAccessRule> urlAccessRules()
    {
        return wrapped.urlAccessRules();
    }

    @Override
    public Iterable<QueryEngineProvider> executionEngines()
    {
        return wrapped.executionEngines();
    }

    @Override
    public Iterable<Pair<DeferredExecutor,Group>> deferredExecutors()
    {
        return wrapped.deferredExecutors();
    }

    @Override
    public AvailabilityGuardInstaller availabilityGuardInstaller()
    {
        return guardInstaller;
    }
}
