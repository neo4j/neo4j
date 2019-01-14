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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.GraphDatabaseDependencies.newDependencies;

public class GraphDatabaseFactoryState
{
    // Keep these fields volatile or equivalent because of this scenario:
    // - one thread creates a GraphDatabaseFactory (including state)
    // - this factory will potentially be handed over to other threads, which will create databases
    private final List<Class<?>> settingsClasses;
    private final List<KernelExtensionFactory<?>> kernelExtensions;
    private volatile Monitors monitors;
    private volatile LogProvider userLogProvider;
    private final Map<String,URLAccessRule> urlAccessRules;

    public GraphDatabaseFactoryState()
    {
        settingsClasses = new CopyOnWriteArrayList<>();
        settingsClasses.add( GraphDatabaseSettings.class );
        kernelExtensions = new CopyOnWriteArrayList<>();
        for ( KernelExtensionFactory<?> factory : Service.load( KernelExtensionFactory.class ) )
        {
            kernelExtensions.add( factory );
        }
        urlAccessRules = new ConcurrentHashMap<>();
    }

    public GraphDatabaseFactoryState( GraphDatabaseFactoryState previous )
    {
        settingsClasses = new CopyOnWriteArrayList<>( previous.settingsClasses );
        kernelExtensions = new CopyOnWriteArrayList<>( previous.kernelExtensions );
        urlAccessRules = new ConcurrentHashMap<>( previous.urlAccessRules );
        monitors = previous.monitors;
        monitors = previous.monitors;
        userLogProvider = previous.userLogProvider;
    }

    public Iterable<KernelExtensionFactory<?>> getKernelExtension()
    {
        return kernelExtensions;
    }

    public void removeKernelExtensions( Predicate<KernelExtensionFactory<?>> toRemove )
    {
        kernelExtensions.removeIf( toRemove );
    }

    public void setKernelExtensions( Iterable<KernelExtensionFactory<?>> newKernelExtensions )
    {
        kernelExtensions.clear();
        addKernelExtensions( newKernelExtensions );
    }

    public void addKernelExtensions( Iterable<KernelExtensionFactory<?>> newKernelExtensions )
    {
        for ( KernelExtensionFactory<?> newKernelExtension : newKernelExtensions )
        {
            kernelExtensions.add( newKernelExtension );
        }
    }

    /**
     * @param settings a class with all settings.
     * @deprecated This method has no side effects now since we moved to service loading instead, {@link LoadableConfig}
     * should be used.
     */
    @Deprecated
    public void addSettingsClasses( Iterable<Class<?>> settings )
    {
        for ( Class<?> setting : settings )
        {
            settingsClasses.add( setting );
        }
    }

    public void addURLAccessRule( String protocol, URLAccessRule rule )
    {
        urlAccessRules.put( protocol, rule );
    }

    public void setUserLogProvider( LogProvider userLogProvider )
    {
        this.userLogProvider = userLogProvider;
    }

    public void setMonitors( Monitors monitors )
    {
        this.monitors = monitors;
    }

    public GraphDatabaseFacadeFactory.Dependencies databaseDependencies()
    {
        return newDependencies().
                monitors( monitors ).
                userLogProvider( userLogProvider ).
                settingsClasses( settingsClasses ).
                urlAccessRules( urlAccessRules ).
                kernelExtensions( kernelExtensions );
    }
}
