/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha.cluster;

import java.net.URI;

import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * A mode switcher deals with how internal services should react when changing
 * between different cluster states, and allows them to switch to the new state
 * that reflects that.
 *
 * @param <T>
 */
public abstract class AbstractModeSwitcher<T> implements Lifecycle
{
    private final DelegateInvocationHandler<T> delegate;
    private LifeSupport life;
    private final HighAvailability highAvailability;
    private DelegateStateSwitcher delegateStateSwitcher;

    protected AbstractModeSwitcher( HighAvailability highAvailability, DelegateInvocationHandler<T> delegate )
    {
        this.delegate = delegate;
        this.life = new LifeSupport();
        this.highAvailability = highAvailability;
        highAvailability.addHighAvailabilityMemberListener( delegateStateSwitcher = new DelegateStateSwitcher() );
    }

    @Override
    public void init() throws Throwable
    {
        life.init();
    }

    @Override
    public void start() throws Throwable
    {
        life.start();
    }

    @Override
    public void stop() throws Throwable
    {
        life.stop();
        highAvailability.removeHighAvailabilityMemberListener( delegateStateSwitcher );
    }

    @Override
    public void shutdown() throws Throwable
    {
        life.shutdown();
    }

    protected abstract T getSlaveImpl( URI serverHaUri );

    protected abstract T getMasterImpl();

    private class DelegateStateSwitcher implements HighAvailabilityMemberListener
    {
        private T current = null;

        @Override
        public void masterIsElected( HighAvailabilityMemberChangeEvent event )
        {
            stateChanged( event );
        }

        @Override
        public void masterIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
            stateChanged( event );
        }

        @Override
        public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
        }

        @Override
        public void instanceStops( HighAvailabilityMemberChangeEvent event )
        {
            stateChanged( event );
        }

        private void stateChanged( HighAvailabilityMemberChangeEvent event )
        {
            if ( event.getNewState() == event.getOldState() )
            {
                return;
            }

            switch ( event.getNewState() )
            {
                case TO_MASTER:
                    shutdownCurrent();
                    delegate.setDelegate( current = life.add( getMasterImpl() ) );
                    life.start();
                    break;
                case TO_SLAVE:
                    shutdownCurrent();
                    delegate.setDelegate( current = life.add( getSlaveImpl( event.getServerHaUri() ) ) );
                    life.start();
                    break;
                case PENDING:
                    shutdownCurrent();
                    break;
            }
        }

        private void shutdownCurrent()
        {
            if ( current != null )
            {
                life.shutdown();
                life = new LifeSupport();
            }
        }
    }
}
