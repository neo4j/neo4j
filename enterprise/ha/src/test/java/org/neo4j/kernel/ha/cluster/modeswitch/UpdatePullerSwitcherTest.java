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

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.MasterUpdatePuller;
import org.neo4j.kernel.ha.PullerFactory;
import org.neo4j.kernel.ha.SlaveUpdatePuller;
import org.neo4j.kernel.ha.UpdatePuller;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class UpdatePullerSwitcherTest
{
    private UpdatePullerSwitcher modeSwitcher;
    private SlaveUpdatePuller slaveUpdatePuller;

    @Before
    public void setUp()
    {
        @SuppressWarnings( "unchecked" )
        DelegateInvocationHandler<UpdatePuller> invocationHandler = mock( DelegateInvocationHandler.class );
        PullerFactory pullerFactory = mock( PullerFactory.class );
        slaveUpdatePuller = mock( SlaveUpdatePuller.class );
        when( pullerFactory.createSlaveUpdatePuller() ).thenReturn( slaveUpdatePuller );
        when( invocationHandler.setDelegate( slaveUpdatePuller ) ).thenReturn( slaveUpdatePuller );

        modeSwitcher = new UpdatePullerSwitcher( invocationHandler, pullerFactory );
    }

    @Test
    public void masterUpdatePuller()
    {
        UpdatePuller masterPuller = modeSwitcher.getMasterImpl();
        assertSame( MasterUpdatePuller.INSTANCE, masterPuller );
    }

    @Test
    public void slaveUpdatePuller() throws Throwable
    {
        UpdatePuller updatePuller = modeSwitcher.getSlaveImpl();
        assertSame( slaveUpdatePuller, updatePuller );
        verifyZeroInteractions( slaveUpdatePuller );
    }

    @Test
    public void switchToPendingTest() throws Exception
    {
        modeSwitcher.switchToSlave();
        verify( slaveUpdatePuller ).start();

        modeSwitcher.switchToSlave();
        InOrder inOrder = inOrder( slaveUpdatePuller );
        inOrder.verify( slaveUpdatePuller ).stop();
        inOrder.verify( slaveUpdatePuller ).start();
    }
}
