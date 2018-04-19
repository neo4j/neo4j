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
package org.neo4j.causalclustering.net;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.causalclustering.net.ServerStateTestHelpers.EnableableState;
import org.neo4j.causalclustering.net.ServerStateTestHelpers.LifeCycleState;

import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.net.ServerStateTestHelpers.LifeCycleState.values;
import static org.neo4j.causalclustering.net.ServerStateTestHelpers.createServer;
import static org.neo4j.causalclustering.net.ServerStateTestHelpers.setEnableableState;
import static org.neo4j.causalclustering.net.ServerStateTestHelpers.setInitialState;

@RunWith( Parameterized.class )
public class ServeLifeCycleStateChangeTest
{
    @Parameterized.Parameter()
    public LifeCycleState fromState;

    @Parameterized.Parameter( 1 )
    public EnableableState fromEnableableState;

    @Parameterized.Parameter( 2 )
    public LifeCycleState toLifeCycleState;

    @Parameterized.Parameter( 3 )
    public boolean shouldBeRunning;

    @Parameterized.Parameters( name = "From {0} and {1} to {2} Server is running ? {3}" )
    public static Iterable<Object[]> data()
    {
        List<Object[]> params = new ArrayList<>();
        for ( LifeCycleState lifeCycleState : values() )
        {
            for ( EnableableState enableableState : EnableableState.values() )
            {
                for ( LifeCycleState toState : lifeCycleOperation() )
                {
                    params.add( new Object[]{lifeCycleState, enableableState, toState, expectedResult( enableableState, toState )} );
                }
            }
        }
        return params;
    }

    private Server server;

    private static LifeCycleState[] lifeCycleOperation()
    {
        return new LifeCycleState[]{LifeCycleState.Start, LifeCycleState.Stop};
    }

    @Before
    public void setUpServer() throws Throwable
    {
        server = createServer();
        setInitialState( server, fromState );
        setEnableableState( server, fromEnableableState );
    }

    @After
    public void shutdown()
    {
        ServerStateTestHelpers.teardown( server );
    }

    @Test
    public void executeEnableable() throws Throwable
    {
        toLifeCycleState.set( server );
        assertEquals( shouldBeRunning, server.isRunnig() );
    }

    private static boolean expectedResult( EnableableState state, LifeCycleState toLifeCycle )
    {
        if ( state == EnableableState.Untouched || state == EnableableState.Enabled )
        {
            if ( toLifeCycle == LifeCycleState.Stop || toLifeCycle == LifeCycleState.Shutdown )
            {
                return false;
            }
            else if ( toLifeCycle == LifeCycleState.Start )
            {
                return true;
            }
            else
            {
                throw new IllegalStateException( "Should not transition to state " + toLifeCycle );
            }
        }
        else if ( state == EnableableState.Disabled )
        {
            return false;
        }
        else
        {
            throw new IllegalStateException( "Unknown state " + state );
        }
    }
}
