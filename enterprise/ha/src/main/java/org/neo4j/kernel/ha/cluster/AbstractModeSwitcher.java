/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
public abstract class AbstractModeSwitcher<T> implements ModeSwitcher, Lifecycle
{
    private final DelegateInvocationHandler<T> delegate;
    private final ModeSwitcherNotifier notifier;
    private T current = null;

    private LifeSupport life = new LifeSupport();

    protected AbstractModeSwitcher( ModeSwitcherNotifier notifier, DelegateInvocationHandler<T> delegate )
    {
        this.notifier = notifier;
        this.delegate = delegate;
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
        notifier.addModeSwitcher( this );
    }

    @Override
    public void stop() throws Throwable
    {
        life.stop();
        notifier.removeModeSwitcher( this );
    }

    @Override
    public void shutdown() throws Throwable
    {
        life.shutdown();
    }

    @Override
    public void switchToMaster()
    {
        shutdownCurrent();
        delegate.setDelegate( current = getMasterImpl( life ) );
        life.start();
    }

    @Override
    public void switchToSlave()
    {
        shutdownCurrent();
        delegate.setDelegate( current = getSlaveImpl( life ) );
        life.start();
    }

    @Override
    public void switchToPending()
    {
        shutdownCurrent();
    }

    private void shutdownCurrent()
    {
        if ( current != null )
        {
            life.shutdown();
            life = new LifeSupport();
        }
    }

    protected abstract T getSlaveImpl( LifeSupport life );
    protected abstract T getMasterImpl( LifeSupport life );
}
