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
package org.neo4j.kernel.ha;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.cluster.InstanceId;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.cluster.ModeSwitcherNotifier;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.slave.InvalidEpochExceptionHandler;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class UpdatePullerModeSwitcherTest
{
    private UpdatePullerModeSwitcher modeSwitcher;

    @Before
    public void setUp()
    {
        ModeSwitcherNotifier switcherNotifier = mock( ModeSwitcherNotifier.class );
        @SuppressWarnings( "unchecked" )
        DelegateInvocationHandler<UpdatePuller> invocationHandler = mock( DelegateInvocationHandler.class );
        PullerFactory pullersFactory = new PullerFactory( mock( RequestContextFactory.class ), mock( Master.class ),
                mock( LastUpdateTime.class ), new AssertableLogProvider(), mock( InstanceId.class ), mock(
                InvalidEpochExceptionHandler.class ), 42, mock( JobScheduler.class ), mock( DependencyResolver.class ),
                mock( AvailabilityGuard.class ), mock( HighAvailabilityMemberStateMachine.class ), new Monitors() );
        modeSwitcher = new UpdatePullerModeSwitcher( switcherNotifier, invocationHandler, pullersFactory );
    }

    @Test
    public void masterUpdatePuller()
    {
        LifeSupport life = mock( LifeSupport.class );
        UpdatePuller masterPuller = modeSwitcher.getMasterImpl( life );
        assertEquals( UpdatePuller.NONE, masterPuller );
        verifyZeroInteractions( life );
    }

    @Test
    public void slaveUpdatePuller()
    {
        LifeSupport life = spy( new LifeSupport() );
        UpdatePuller slavePuller = modeSwitcher.getSlaveImpl( life );
        assertNotNull( slavePuller );
        verify( life, times( 1 ) ).add( (Lifecycle) slavePuller );
    }
}
