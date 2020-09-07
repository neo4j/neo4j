/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.security.URLAccessRules;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.service.Services;

import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.internal.helpers.collection.Iterables.empty;

public class GraphDatabaseDependencies implements ExternalDependencies
{
    public static GraphDatabaseDependencies newDependencies( ExternalDependencies deps )
    {
        return new GraphDatabaseDependencies( deps.monitors(), deps.userLogProvider(), deps.dependencies(), deps.extensions(),
                deps.urlAccessRules(), deps.databaseEventListeners() );
    }

    public static GraphDatabaseDependencies newDependencies()
    {
        Iterable<ExtensionFactory<?>> extensions = getExtensions( Services.loadAll( ExtensionFactory.class ).iterator() );

        Map<String,URLAccessRule> urlAccessRules = new HashMap<>();
        urlAccessRules.put( "http", URLAccessRules.alwaysPermitted() );
        urlAccessRules.put( "https", URLAccessRules.alwaysPermitted() );
        urlAccessRules.put( "ftp", URLAccessRules.alwaysPermitted() );
        urlAccessRules.put( "file", URLAccessRules.fileAccess() );

        return new GraphDatabaseDependencies( null, null, null, extensions,
                urlAccessRules, empty() );
    }

    private Monitors monitors;
    private LogProvider userLogProvider;
    private DependencyResolver dependencies;
    private List<ExtensionFactory<?>> extensions;
    private List<DatabaseEventListener> databaseEventListeners;
    private final Map<String,URLAccessRule> urlAccessRules;

    private GraphDatabaseDependencies(
            Monitors monitors,
            LogProvider userLogProvider,
            DependencyResolver dependencies,
            Iterable<ExtensionFactory<?>> extensions,
            Map<String,URLAccessRule> urlAccessRules,
            Iterable<DatabaseEventListener> eventListeners
            )
    {
        this.monitors = monitors;
        this.userLogProvider = userLogProvider;
        this.dependencies = dependencies;
        this.extensions = asList( extensions );
        this.urlAccessRules = urlAccessRules;
        this.databaseEventListeners = asList( eventListeners );
    }

    // Builder DSL
    public GraphDatabaseDependencies monitors( Monitors monitors )
    {
        this.monitors = monitors;
        return this;
    }

    public GraphDatabaseDependencies userLogProvider( LogProvider userLogProvider )
    {
        this.userLogProvider = userLogProvider;
        return this;
    }

    public GraphDatabaseDependencies dependencies( DependencyResolver dependencies )
    {
        this.dependencies = dependencies;
        return this;
    }

    public GraphDatabaseDependencies databaseEventListeners( Iterable<DatabaseEventListener> eventListeners )
    {
        this.databaseEventListeners = asList( eventListeners );
        return this;
    }

    public GraphDatabaseDependencies extensions( Iterable<ExtensionFactory<?>> extensions )
    {
        this.extensions = asList( extensions );
        return this;
    }

    public GraphDatabaseDependencies urlAccessRules( Map<String,URLAccessRule> urlAccessRules )
    {
        this.urlAccessRules.putAll( urlAccessRules );
        return this;
    }

    // Dependencies implementation
    @Override
    public Monitors monitors()
    {
        return monitors;
    }

    @Override
    public LogProvider userLogProvider()
    {
        return userLogProvider;
    }

    @Override
    public Iterable<ExtensionFactory<?>> extensions()
    {
        return extensions;
    }

    @Override
    public Map<String,URLAccessRule> urlAccessRules()
    {
        return urlAccessRules;
    }

    @Override
    public Iterable<DatabaseEventListener> databaseEventListeners()
    {
        return databaseEventListeners;
    }

    @Override
    public DependencyResolver dependencies()
    {
        return dependencies;
    }

    // This method is needed to convert the non generic ExtensionFactory type returned from Service.load
    // to ExtensionFactory<?> generic types
    private static Iterable<ExtensionFactory<?>> getExtensions( Iterator<ExtensionFactory> parent )
    {
        return Iterators.asList( new Iterator<>()
        {
            @Override
            public boolean hasNext()
            {
                return parent.hasNext();
            }

            @Override
            public ExtensionFactory<?> next()
            {
                return parent.next();
            }
        } );
    }
}
