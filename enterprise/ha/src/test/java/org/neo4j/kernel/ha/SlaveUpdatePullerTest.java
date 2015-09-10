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
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.InvalidEpochException;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.slave.InvalidEpochExceptionHandler;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.CleanupRule;

import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.ha.SlaveUpdatePuller.Condition;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class SlaveUpdatePullerTest
{
    private final InstanceId instanceId = new InstanceId( 1 );
    private final Config config = mock( Config.class );
    private final AvailabilityGuard availabilityGuard = mock( AvailabilityGuard.class );
    private final LastUpdateTime lastUpdateTime = mock( LastUpdateTime.class );
    private final Master master = mock( Master.class );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final RequestContextFactory requestContextFactory = mock( RequestContextFactory.class );
    private final InvalidEpochExceptionHandler invalidEpochHandler = mock( InvalidEpochExceptionHandler.class );
    private final SlaveUpdatePuller updatePuller = new SlaveUpdatePuller( requestContextFactory,
            master, lastUpdateTime, logProvider, instanceId, availabilityGuard, invalidEpochHandler );

    @Rule
    public final CleanupRule cleanup = new CleanupRule();

    @Before
    public void setup() throws Throwable
    {
        when( config.get( HaSettings.pull_interval ) ).thenReturn( 1000l );
        when( config.get( ClusterSettings.server_id ) ).thenReturn( instanceId );
        when( availabilityGuard.isAvailable( anyLong() ) ).thenReturn( true );
        updatePuller.init();
        updatePuller.start();
    }

    @Test
    public void shouldStopPullingAfterStop() throws Throwable
    {
        // WHEN
        updatePuller.pullUpdates();

        // THEN
        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        // WHEN
        updatePuller.shutdown();
        updatePuller.pullUpdates();

        // THEN
        verifyNoMoreInteractions( lastUpdateTime, availabilityGuard );
    }

    @Test
    public void keepPullingUpdatesOnConsecutiveCalls() throws Throwable
    {
        // WHEN
        updatePuller.pullUpdates();

        // THEN
        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        // WHEN
        updatePuller.pullUpdates();

        // THEN
        verify( lastUpdateTime, times( 2 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 2 ) ).isAvailable( anyLong() );
        verify( master, times( 2 ) ).pullUpdates( Matchers.<RequestContext>any() );
    }

    @Test
    public void falseOnTryPullUpdatesOnInactivePuller() throws Throwable
    {
        // GIVEN
        updatePuller.shutdown();

        // WHEN
        boolean result = updatePuller.tryPullUpdates();

        // THEN
        assertFalse( result );
    }

    @Test
    public void shouldThrowIfPullerInitiallyInactiveStrict() throws Throwable
    {
        // GIVEN
        Condition condition = mock( Condition.class );
        updatePuller.shutdown();

        // WHEN
        try
        {
            updatePuller.pullUpdates( condition, true );
            fail( "Should have thrown" );
        }
        catch ( IllegalStateException e )
        {   // THEN Good
            verifyNoMoreInteractions( condition );
        }
    }

    @Test
    public void shouldThrowIfPullerBecomesInactiveWhileWaitingStrict() throws Exception
    {
        // GIVEN
        Condition condition = mock( Condition.class );

        when( condition.evaluate( anyInt(), anyInt() ) ).thenAnswer( new Answer<Boolean>()
        {
            @Override
            public Boolean answer( InvocationOnMock invocation ) throws Throwable
            {
                updatePuller.shutdown();
                return false;
            }
        } );

        // WHEN
        try
        {
            updatePuller.pullUpdates( condition, true );
            fail( "Should have thrown" );
        }
        catch ( IllegalStateException e )
        {   // THEN Good
            verify( condition ).evaluate( anyInt(), anyInt() );
        }
    }

    @Test
    public void shouldHandleInvalidEpochByNotifyingItsHandler() throws Exception
    {
        // GIVEN
        doThrow( InvalidEpochException.class ).when( master ).pullUpdates( any( RequestContext.class ) );

        // WHEN
        updatePuller.pullUpdates();

        // THEN
        verify( invalidEpochHandler ).handle();
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldCopeWithHardExceptionsLikeOutOfMemory() throws Exception
    {
        // GIVEN
        OutOfMemoryError oom = new OutOfMemoryError();
        when( master.pullUpdates( any( RequestContext.class ) ) )
                .thenThrow( oom )
                .thenReturn( Response.EMPTY );

        // WHEN making the first pull
        updatePuller.pullUpdates();

        // THEN the OOM should be caught and logged
        logProvider.assertAtLeastOnce(
                inLog( SlaveUpdatePuller.class ).error( org.hamcrest.Matchers.any( String.class ), sameInstance( oom ) )
        );

        // WHEN that has passed THEN we should still be making pull attempts.
        updatePuller.pullUpdates();
    }
}
