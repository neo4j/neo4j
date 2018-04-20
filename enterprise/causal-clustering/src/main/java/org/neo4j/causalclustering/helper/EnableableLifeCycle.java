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
package org.neo4j.causalclustering.helper;

import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;

public abstract class EnableableLifeCycle implements Lifecycle, Enableable
{
    private final Log debugLog;
    private boolean stoppedByLifeCycle = true;
    private boolean enabled = true;

    public EnableableLifeCycle( Log debugLog )
    {
        this.debugLog = debugLog;
    }

    @Override
    public void enable()
    {
        enabled = true;
        if ( !stoppedByLifeCycle )
        {
            start0();
        }
        else
        {
            debugLog.info( "%s will not start. It was enabled but is stopped by lifecycle", this );
        }
    }

    @Override
    public void disable()
    {
        enabled = false;
        stop0();
    }

    @Override
    public void init()
    {
        init0();
    }

    @Override
    public void start()
    {
        stoppedByLifeCycle = false;
        if ( !enabled )
        {
            debugLog.info( "Start call from lifecycle is ignored because %s is disabled.", this );
        }
        else
        {
            start0();
        }
    }

    @Override
    public void stop()
    {
        stoppedByLifeCycle = true;
        stop0();
    }

    @Override
    public void shutdown()
    {
        stoppedByLifeCycle = true;
        shutdown0();
    }

    protected abstract void init0();

    protected abstract void start0();

    protected abstract void stop0();

    protected abstract void shutdown0();
}
