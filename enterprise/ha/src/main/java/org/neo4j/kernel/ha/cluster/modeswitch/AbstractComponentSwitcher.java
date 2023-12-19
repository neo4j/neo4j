/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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

    protected T getPendingImpl()
    {
        return null;
    }

    @Override
    public final void switchToMaster()
    {
        updateDelegate( getMasterImpl() );
    }

    @Override
    public final void switchToSlave()
    {
        updateDelegate( getSlaveImpl() );
    }

    @Override
    public final void switchToPending()
    {
        updateDelegate( getPendingImpl() );
    }

    private void updateDelegate( T newValue )
    {
        T oldDelegate = delegate.setDelegate( newValue );
        shutdownOldDelegate( oldDelegate );
        startNewDelegate( newValue );
    }

    protected void startNewDelegate( T newValue )
    {
        // no-op by default
    }

    protected void shutdownOldDelegate( T oldDelegate )
    {
        // no-op by default
    }
}
