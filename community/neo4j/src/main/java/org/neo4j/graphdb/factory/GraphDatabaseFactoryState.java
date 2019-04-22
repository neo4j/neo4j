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
package org.neo4j.graphdb.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.service.Services;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.graphdb.facade.GraphDatabaseDependencies.newDependencies;

/**
 * @deprecated This will be moved to an internal package in the future.
 */
@Deprecated
public class GraphDatabaseFactoryState
{
    private final List<Class<?>> settingsClasses = new ArrayList<>();
    private final List<ExtensionFactory<?>> extensions = new ArrayList<>();
    private Monitors monitors;
    private LogProvider userLogProvider;
    private DependencyResolver dependencies = new Dependencies();
    private final Map<String,URLAccessRule> urlAccessRules = new HashMap<>();

    public GraphDatabaseFactoryState()
    {
        settingsClasses.add( GraphDatabaseSettings.class );
        Services.loadAll( ExtensionFactory.class ).forEach( extensions::add );
    }

    @VisibleForTesting
    synchronized Iterable<ExtensionFactory<?>> getExtension()
    {
        return new ArrayList<>( extensions );
    }

    public synchronized void removeExtensions( Predicate<ExtensionFactory<?>> toRemove )
    {
        extensions.removeIf( toRemove );
    }

    public synchronized void setExtensions( Iterable<ExtensionFactory<?>> newExtensions )
    {
        extensions.clear();
        addExtensions( newExtensions );
    }

    public synchronized void addExtensions( Iterable<ExtensionFactory<?>> extensions )
    {
        for ( ExtensionFactory<?> extension : extensions )
        {
            this.extensions.add( extension );
        }
    }

    public synchronized void addURLAccessRule( String protocol, URLAccessRule rule )
    {
        urlAccessRules.put( protocol, rule );
    }

    public synchronized void setUserLogProvider( LogProvider userLogProvider )
    {
        this.userLogProvider = userLogProvider;
    }

    public synchronized void setMonitors( Monitors monitors )
    {
        this.monitors = monitors;
    }

    public synchronized void setDependencies( DependencyResolver dependencies )
    {
        this.dependencies = dependencies;
    }

    public synchronized ExternalDependencies databaseDependencies()
    {
        return newDependencies().
                monitors( monitors ).
                userLogProvider( userLogProvider ).
                dependencies( dependencies ).
                settingsClasses( settingsClasses ).
                urlAccessRules( urlAccessRules ).
                extensions( extensions );
    }
}
