/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster.modeswitch;

import org.neo4j.kernel.ha.DelegateInvocationHandler;

/**
 * A component switcher deals with how internal services should react when changing
 * between different cluster states, and allows them to switch to the new state
 * that reflects that.
 *
 * @param <T>
 */
public abstract class AbstractComponentSwitcher<T> implements ComponentSwitcher
{
    private final DelegateInvocationHandler<T> delegate;

    protected AbstractComponentSwitcher( DelegateInvocationHandler<T> delegate )
    {
        this.delegate = delegate;
    }

    protected abstract T getMasterImpl();

    protected abstract T getSlaveImpl();

    @Override
    public void switchToMaster()
    {
        shutdownCurrent();
        T component = getMasterImpl();
        updateDelegate( component );
    }

    @Override
    public void switchToSlave()
    {
        shutdownCurrent();
        T component = getSlaveImpl();
        updateDelegate( component );
    }

    @Override
    public void switchToPending()
    {
        shutdownCurrent();
    }

    protected void shutdownCurrent()
    {
        updateDelegate( null );
    }

    private void updateDelegate( T newValue )
    {
        delegate.setDelegate( newValue );
    }
}
