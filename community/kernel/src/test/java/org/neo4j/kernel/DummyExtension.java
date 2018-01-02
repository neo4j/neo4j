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
package org.neo4j.kernel;

import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleStatus;

public class DummyExtension implements Lifecycle
{
    LifecycleStatus status = LifecycleStatus.NONE;
    private DummyExtensionFactory.Dependencies dependencies;

    public DummyExtension( DummyExtensionFactory.Dependencies dependencies )
    {
        this.dependencies = dependencies;
    }

    @Override
    public void init() throws Throwable
    {
        if ( status != LifecycleStatus.NONE )
        {
            throw new IllegalStateException( "Wrong state:" + status );
        }

        status = LifecycleStatus.STOPPED;
    }

    @Override
    public void start() throws Throwable
    {
        if ( status != LifecycleStatus.STOPPED )
        {
            throw new IllegalStateException( "Wrong state:" + status );
        }

        status = LifecycleStatus.STARTED;
    }

    @Override
    public void stop() throws Throwable
    {
        if ( status != LifecycleStatus.STARTED )
        {
            throw new IllegalStateException( "Wrong state:" + status );
        }

        status = LifecycleStatus.STOPPED;
    }

    @Override
    public void shutdown() throws Throwable
    {
        if ( status != LifecycleStatus.STOPPED )
        {
            throw new IllegalStateException( "Wrong state:" + status );
        }

        status = LifecycleStatus.SHUTDOWN;
    }

    public LifecycleStatus getStatus()
    {
        return status;
    }

    public DummyExtensionFactory.Dependencies getDependencies()
    {
        return dependencies;
    }
}
