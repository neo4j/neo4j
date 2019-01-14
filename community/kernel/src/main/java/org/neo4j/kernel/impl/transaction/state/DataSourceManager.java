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

import java.util.function.Supplier;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Listeners;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleStatus;

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
    private NeoStoreDataSource dataSource;

    public void addListener( Listener listener )
    {
        if ( life.getStatus().equals( LifecycleStatus.STARTED ) )
        {
            try
            {
                if ( dataSource != null )
                {
                    listener.registered( dataSource );
                }
            }
            catch ( Throwable t )
            {   // OK
            }
        }
        dsRegistrationListeners.add( listener );
    }

    public void register( NeoStoreDataSource dataSource )
    {
        this.dataSource = dataSource;
        if ( life.getStatus().equals( LifecycleStatus.STARTED ) )
        {
            dsRegistrationListeners.notify( listener -> listener.registered( dataSource ) );
        }
    }

    public void unregister( NeoStoreDataSource dataSource )
    {
        this.dataSource = null;
        dsRegistrationListeners.notify( listener -> listener.unregistered( dataSource ) );
        life.remove( dataSource );
    }

    public NeoStoreDataSource getDataSource()
    {
        return dataSource;
    }

    @Override
    public void init()
    {
        life = new LifeSupport();
        life.add( dataSource );
    }

    @Override
    public void start()
    {
        life.start();

        for ( Listener listener : dsRegistrationListeners )
        {
            try
            {
                if ( dataSource != null )
                {
                    listener.registered( dataSource );
                }
            }
            catch ( Throwable t )
            {   // OK
            }
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
        dataSource = null;
    }

    @Override
    public Kernel get()
    {
        return dataSource.getKernel();
    }

    public static class DependencyResolverSupplier implements Supplier<DependencyResolver>
    {
        private DataSourceManager dataSourceManager;

        public DependencyResolverSupplier( DataSourceManager dataSourceManager )
        {
            this.dataSourceManager = dataSourceManager;
        }

        @Override
        public DependencyResolver get()
        {
            NeoStoreDataSource dataSource = dataSourceManager.getDataSource();
            if ( dataSource == null )
            {
                return null;
            }
            return dataSource.getDependencyResolver();
        }
    }
}
