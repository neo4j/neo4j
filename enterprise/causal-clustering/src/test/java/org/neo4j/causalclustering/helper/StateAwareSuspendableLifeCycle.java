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

import org.neo4j.causalclustering.helper.SuspendableLifecycleStateTestHelpers.LifeCycleState;
import org.neo4j.logging.Log;

public class StateAwareSuspendableLifeCycle extends SuspendableLifeCycle
{
    public LifeCycleState status;

    StateAwareSuspendableLifeCycle( Log debugLog )
    {
        super( debugLog );
    }

    @Override
    protected void start0()
    {
        status = LifeCycleState.Start;
    }

    @Override
    protected void stop0()
    {
        status = LifeCycleState.Stop;
    }

    @Override
    protected void shutdown0()
    {
        status = LifeCycleState.Shutdown;
    }

    @Override
    protected void init0()
    {
        status = LifeCycleState.Init;
    }
}
