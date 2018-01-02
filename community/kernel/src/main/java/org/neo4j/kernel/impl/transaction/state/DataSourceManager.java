/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.function.Supplier;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleStatus;

/**
 * Adds change listener features to a {@link NeoStoreDataSource}.
 * <p/>
 * TODO This being a {@link KernelAPI} {@link Supplier} is a smell, it comes from established bad dependency hierarchy
 * where {@link NeoStoreDataSource} and {@link KernelAPI} are needed before they exist.
 */
public class DataSourceManager implements Lifecycle, Supplier<KernelAPI>
{
    public interface Listener
    {
        void registered( NeoStoreDataSource dataSource );

        void unregistered( NeoStoreDataSource dataSource );
    }

    private LifeSupport life = new LifeSupport();
    private Iterable<Listener> dsRegistrationListeners = Listeners.newListeners();
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
        dsRegistrationListeners = Listeners.addListener( listener, dsRegistrationListeners );
    }

    public void register( final NeoStoreDataSource dataSource )
    {
        this.dataSource = dataSource;
        if ( life.getStatus().equals( LifecycleStatus.STARTED ) )
        {
            Listeners.notifyListeners( dsRegistrationListeners, new Listeners.Notification<Listener>()
            {
                @Override
                public void notify( Listener listener )
                {
                    listener.registered( dataSource );
                }
            } );
        }
    }

    public void unregister( final NeoStoreDataSource dataSource )
    {
        this.dataSource = null;
        Listeners.notifyListeners( dsRegistrationListeners, new Listeners.Notification<Listener>()
        {
            @Override
            public void notify( Listener listener )
            {
                listener.unregistered( dataSource );
            }
        } );
        life.remove( dataSource );
    }

    public NeoStoreDataSource getDataSource()
    {
        return dataSource;
    }

    @Override
    public void init() throws Throwable
    {
        life = new LifeSupport();
        life.add( dataSource );
    }

    @Override
    public void start() throws Throwable
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
    public void stop() throws Throwable
    {
        life.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        life.shutdown();
        dataSource = null;
    }

    @Override
    public KernelAPI get()
    {
        return dataSource.getKernel();
    }
}
