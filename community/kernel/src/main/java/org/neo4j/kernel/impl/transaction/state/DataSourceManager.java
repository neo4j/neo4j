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
package org.neo4j.kernel.impl.transaction.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Listeners;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

/**
 * Adds change listener features to a {@link NeoStoreDataSource}.
 * <p/>
 * TODO This being a {@link Kernel} {@link Supplier} is a smell, it comes from established bad dependency hierarchy
 * where {@link NeoStoreDataSource} and {@link Kernel} are needed before they exist.
 */
public class DataSourceManager implements Lifecycle, Supplier<Kernel>
{
    public interface Listener
    {
        void registered( NeoStoreDataSource dataSource );

        void unregistered( NeoStoreDataSource dataSource );
    }

    private LifeSupport life = new LifeSupport();
    private final Listeners<Listener> dsRegistrationListeners = new Listeners<>();
    private final Map<String,NeoStoreDataSource> dataSources = new HashMap<>();
    private final Log log;
    private final Config config;

    public DataSourceManager( LogProvider logProvider, Config config )
    {
        this.log = logProvider.getLog( this.getClass() );
        this.config = config;
    }

    private void registerListener( Listener listener )
    {
        try
        {
            dataSources.values().forEach( listener::registered );
        }
        catch ( Throwable cause )
        {
            log.error( format( "Error when registering listener %s to datasources", listener ), cause );
        }
    }

    public void addListener( Listener listener )
    {
        if ( life.getStatus().equals( LifecycleStatus.STARTED ) )
        {
            registerListener( listener );
        }
        dsRegistrationListeners.add( listener );
    }

    public void register( String name, NeoStoreDataSource dataSource )
    {
        dataSources.put( name, dataSource );
        if ( life.getStatus().equals( LifecycleStatus.STARTED ) )
        {
            life.add( dataSource );
            dsRegistrationListeners.notify( listener -> listener.registered( dataSource ) );
        }
    }

    public void unregister( String name )
    {
        NeoStoreDataSource dataSource = dataSources.remove( name );
        dsRegistrationListeners.notify( listener -> listener.unregistered( dataSource ) );
        life.remove( dataSource );
    }

    public NeoStoreDataSource getDataSource()
    {
        return Optional.ofNullable( dataSources.get( config.get( GraphDatabaseSettings.active_database ) ) )
                .orElseThrow( () -> new IllegalStateException( "Default database not found" ) );
    }

    public Optional<NeoStoreDataSource> getDataSource( String name )
    {
        return Optional.ofNullable( dataSources.get( name ) );
    }

    @Override
    public void init()
    {
        life = new LifeSupport();
        dataSources.values().forEach( life::add );
    }

    @Override
    public void start()
    {
        life.start();

        for ( Listener listener : dsRegistrationListeners )
        {
            registerListener( listener );
        }
    }

    @Override
    public void stop()
    {
        life.stop();
    }

    @Override
    public void shutdown()
    {
        life.shutdown();
        dataSources.clear();
    }

    @Override
    public Kernel get()
    {
        return getDataSource().getKernel();
    }
}
