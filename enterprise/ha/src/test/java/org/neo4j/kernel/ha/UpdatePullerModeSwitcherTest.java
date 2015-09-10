/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.kernel.ha.cluster.ModeSwitcherNotifier;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UpdatePullerModeSwitcherTest
{

    private UpdatePullerModeSwitcher modeSwitcher;
    private PullerFactory pullersFactory;
    private DelegateInvocationHandler invocationHandler;
    private ModeSwitcherNotifier switcherNotifier;

    @Before
    public void setUp()
    {
        switcherNotifier = mock( ModeSwitcherNotifier.class );
        invocationHandler = mock( DelegateInvocationHandler.class );
        pullersFactory = mock( PullerFactory.class );
        modeSwitcher = new UpdatePullerModeSwitcher( switcherNotifier, invocationHandler, pullersFactory );
    }

    @Test
    public void masterUpdatePuller()
    {
        UpdatePuller masterPuller = modeSwitcher.getMasterImpl();
        assertEquals( UpdatePuller.NONE, masterPuller );
    }

    @Test
    public void slaveUpdatePuller()
    {
        UpdatePuller updatePuller = mock( UpdatePuller.class );
        when( pullersFactory.createUpdatePuller() ).thenReturn( updatePuller );

        UpdatePuller slavePuller = modeSwitcher.getSlaveImpl();
        assertEquals( updatePuller, slavePuller );
    }

}