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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.causalclustering.helper.EnableableLifecycleStateTestHelpers.EnableableState;
import org.neo4j.causalclustering.helper.EnableableLifecycleStateTestHelpers.LifeCycleState;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.helper.EnableableLifecycleStateTestHelpers.setInitialState;

@RunWith( Parameterized.class )
public class EnableableLifeCylcleEnableableStateChangeTest
{
    @Parameterized.Parameter()
    public LifeCycleState fromState;

    @Parameterized.Parameter( 1 )
    public EnableableState fromEnableableState;

    @Parameterized.Parameter( 2 )
    public EnableableState toEnableableState;

    @Parameterized.Parameter( 3 )
    public LifeCycleState shouldEndInState;

    @Parameterized.Parameters( name = "From {0} and {1} to {2} should end in {3}" )
    public static Iterable<Object[]> data()
    {
        List<Object[]> params = new ArrayList<>();
        for ( LifeCycleState lifeCycleState : LifeCycleState.values() )
        {
            for ( EnableableState enableableState : EnableableState.values() )
            {
                for ( EnableableState toEnableable : toEnableableState() )
                {
                    params.add( new Object[]{lifeCycleState, enableableState, toEnableable, expectedResult( lifeCycleState, enableableState, toEnableable )} );
                }
            }
        }
        return params;
    }

    private StateAwareEnableableLifeCycle lifeCycle;

    private static EnableableState[] toEnableableState()
    {
        return new EnableableState[]{EnableableState.Enabled, EnableableState.Disabled};
    }

    @Before
    public void setUpServer() throws Throwable
    {
        lifeCycle = new StateAwareEnableableLifeCycle( NullLogProvider.getInstance().getLog( "log" ) );
        setInitialState( lifeCycle, fromState );
        fromEnableableState.set( lifeCycle );
    }

    @Test
    public void executeEnableable() throws Throwable
    {
        toEnableableState.set( lifeCycle );
        assertEquals( shouldEndInState, lifeCycle.status );
    }

    private static LifeCycleState expectedResult( LifeCycleState fromState, EnableableState fromEnableableState, EnableableState toEnableable )
    {
        if ( toEnableable == EnableableState.Disabled )
        {
            return LifeCycleState.Stop;
        }
        else if ( toEnableable == EnableableState.Enabled )
        {
            if ( fromEnableableState == EnableableState.Disabled )
            {
                if ( fromState == LifeCycleState.Init || fromState == LifeCycleState.Shutdown )
                {
                    return LifeCycleState.Stop;
                }
            }
            return fromState;
        }
        else
        {
            throw new IllegalStateException( "Should not transition to any other state got: " + toEnableable );
        }
    }
}
