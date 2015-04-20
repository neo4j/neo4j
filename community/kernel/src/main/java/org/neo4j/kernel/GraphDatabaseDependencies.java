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
package org.neo4j.kernel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.helpers.collection.Iterables.addAll;

public class GraphDatabaseDependencies implements InternalAbstractGraphDatabase.Dependencies
{
    public static GraphDatabaseDependencies newDependencies()
    {
        List<KernelExtensionFactory<?>> kernelExtensions = new ArrayList<>();
        for ( KernelExtensionFactory factory : Service.load( KernelExtensionFactory.class ) )
        {
            kernelExtensions.add( factory );
        }
        return new GraphDatabaseDependencies( null, null, new ArrayList<Class<?>>(), kernelExtensions,
                                              Iterables.toList( Service.load( CacheProvider.class ) ),
                                              Iterables.toList( Service.load( QueryEngineProvider.class ) ) );
    }

    private final Monitors monitors;
    private final Logging logging;
    private final List<Class<?>> settingsClasses;
    private final List<KernelExtensionFactory<?>> kernelExtensions;
    private final List<CacheProvider> cacheProviders;
    private final List<QueryEngineProvider> queryEngineProviders;

    private GraphDatabaseDependencies(
            Monitors monitors,
            Logging logging,
            List<Class<?>> settingsClasses,
            List<KernelExtensionFactory<?>> kernelExtensions,
            List<CacheProvider> cacheProviders,
            List<QueryEngineProvider> queryEngineProviders )
    {
        this.monitors = monitors;
        this.logging = logging;
        this.settingsClasses = settingsClasses;
        this.kernelExtensions = kernelExtensions;
        this.cacheProviders = cacheProviders;
        this.queryEngineProviders = queryEngineProviders;
    }

    // Builder DSL
    public GraphDatabaseDependencies monitors( Monitors monitors )
    {
        return new GraphDatabaseDependencies( monitors, logging, settingsClasses, kernelExtensions, cacheProviders,
                                              queryEngineProviders );
    }

    public GraphDatabaseDependencies logging( Logging logging )
    {
        return new GraphDatabaseDependencies( monitors, logging, settingsClasses, kernelExtensions, cacheProviders,
                                              queryEngineProviders );
    }

    public GraphDatabaseDependencies settingsClasses( List<Class<?>> settingsClasses )
    {
        return new GraphDatabaseDependencies( monitors, logging, settingsClasses, kernelExtensions, cacheProviders,
                                              queryEngineProviders );
    }

    public GraphDatabaseDependencies settingsClasses( Class<?>... settingsClass )
    {
        settingsClasses.addAll( Arrays.asList( settingsClass ) );
        return new GraphDatabaseDependencies( monitors, logging, settingsClasses, kernelExtensions, cacheProviders,
                                              queryEngineProviders );
    }

    public GraphDatabaseDependencies kernelExtensions( Iterable<KernelExtensionFactory<?>> kernelExtensions )
    {
        return new GraphDatabaseDependencies( monitors, logging, settingsClasses,
                                              addAll( new ArrayList<KernelExtensionFactory<?>>(), kernelExtensions ),
                                              cacheProviders, queryEngineProviders );
    }

    public GraphDatabaseDependencies cacheProviders( Iterable<CacheProvider> cacheProviders )
    {
        return new GraphDatabaseDependencies( monitors, logging, settingsClasses, kernelExtensions,
                                              addAll( this.cacheProviders, cacheProviders ), queryEngineProviders );
    }

    public GraphDatabaseDependencies queryEngineProviders( Iterable<QueryEngineProvider> queryEngineProviders )
    {
        return new GraphDatabaseDependencies( monitors, logging, settingsClasses, kernelExtensions, cacheProviders,
                                              addAll( this.queryEngineProviders, queryEngineProviders ) );
    }

    // Dependencies implementation
    @Override
    public Monitors monitors()
    {
        return monitors;
    }

    @Override
    public Logging logging()
    {
        return logging;
    }

    @Override
    public Iterable<Class<?>> settingsClasses()
    {
        return settingsClasses;
    }

    @Override
    public Iterable<KernelExtensionFactory<?>> kernelExtensions()
    {
        return kernelExtensions;
    }

    @Override
    public Iterable<CacheProvider> cacheProviders()
    {
        return cacheProviders;
    }

    @Override
    public Iterable<QueryEngineProvider> executionEngines()
    {
        return queryEngineProviders;
    }
}
