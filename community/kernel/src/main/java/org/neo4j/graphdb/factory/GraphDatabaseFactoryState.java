/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import java.util.List;

import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.InternalAbstractGraphDatabase.Dependencies;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.GraphDatabaseDependencies.newDependencies;

public class GraphDatabaseFactoryState
{
    private final List<Class<?>> settingsClasses;
    private final List<KernelExtensionFactory<?>> kernelExtensions;
    private final List<CacheProvider> cacheProviders;
    private Logging logging;
    private Monitors monitors;

    public GraphDatabaseFactoryState() {
        settingsClasses = new ArrayList<>();
        settingsClasses.add( GraphDatabaseSettings.class );
        kernelExtensions = new ArrayList<>();
        for ( KernelExtensionFactory factory : Service.load( KernelExtensionFactory.class ) )
        {
            kernelExtensions.add( factory );
        }
        cacheProviders = Iterables.toList( Service.load( CacheProvider.class ) );
    }

    public GraphDatabaseFactoryState( GraphDatabaseFactoryState previous )
    {
        settingsClasses = new ArrayList<>( previous.settingsClasses );
        kernelExtensions = new ArrayList<>( previous.kernelExtensions );
        cacheProviders = new ArrayList<>( previous.cacheProviders );
        monitors = previous.monitors;
        logging = previous.logging;
        monitors = previous.monitors;
    }

    public Iterable<KernelExtensionFactory<?>> getKernelExtension()
    {
        return kernelExtensions;
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

    public List<CacheProvider> getCacheProviders()
    {
        return cacheProviders;
    }

    public void setCacheProviders( Iterable<CacheProvider> newCacheProviders )
    {
        cacheProviders.clear();
        for ( CacheProvider newCacheProvider : newCacheProviders )
        {
            cacheProviders.add( newCacheProvider );
        }
    }

    public void setLogging( Logging logging )
    {
        this.logging = logging;
    }

    public void setMonitors(Monitors monitors)
    {
        this.monitors = monitors;
    }

    public Dependencies databaseDependencies()
    {
        return newDependencies().
                monitors(monitors).
                logging(logging).
                settingsClasses(settingsClasses).
                kernelExtensions(kernelExtensions).
                cacheProviders(cacheProviders);
    }
}
