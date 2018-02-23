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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import org.neo4j.cluster.InstanceId;
import org.neo4j.function.Suppliers;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UpdatePullingTransactionObligationFulfillerTest
{
    private final UpdatePuller updatePuller = mock( UpdatePuller.class );
    private final HighAvailabilityMemberStateMachine machine = mock( HighAvailabilityMemberStateMachine.class );
    private final InstanceId serverId = new InstanceId( 42 );

    @BeforeEach
    public void setup() throws Throwable
    {
        doAnswer( invocation -> ((UpdatePuller.Condition) invocation.getArgument( 0 )).evaluate( 33, 34 )
        ).when( updatePuller ).pullUpdates( any( UpdatePuller.Condition.class ), anyBoolean() );
    }

    @Test
    public void shouldNotThrowNPEWhenAskedToFulFilledButNotYetHavingARoleAssigned() throws Throwable
    {
        // Given
        UpdatePullingTransactionObligationFulfiller fulfiller =
                new UpdatePullingTransactionObligationFulfiller( updatePuller, machine, serverId,
                        Suppliers.singleton( mock( TransactionIdStore.class ) ) );

        // When
        fulfiller.fulfill( 1 );

        // Then
        // it doesn't blow up
    }

    @Test
    public void shouldUpdateTransactionIdStoreCorrectly() throws Throwable
    {
        // Given
        TransactionIdStore store1 = mock( TransactionIdStore.class );
        TransactionIdStore store2 = mock( TransactionIdStore.class );
        @SuppressWarnings( "unchecked" )
        Supplier<TransactionIdStore> supplier = mock( Supplier.class );
        when( supplier.get() ).thenReturn( store1, store2 );

        doAnswer( invocation ->
        {
            ((HighAvailabilityMemberListener) invocation.getArgument( 0 )).slaveIsAvailable(
                    new HighAvailabilityMemberChangeEvent( null, null, serverId, null )
            );
            return null;
        } ).when( machine ).addHighAvailabilityMemberListener( any( HighAvailabilityMemberListener.class ) );

        doAnswer( invocation ->
        {
            ((HighAvailabilityMemberListener) invocation.getArgument( 0 )).instanceStops(
                    new HighAvailabilityMemberChangeEvent( null, null, serverId, null )
            );
            return null;
        } ).when( machine ).removeHighAvailabilityMemberListener( any( HighAvailabilityMemberListener.class ) );

        UpdatePullingTransactionObligationFulfiller fulfiller =
                new UpdatePullingTransactionObligationFulfiller( updatePuller, machine, serverId, supplier );

        // When
        fulfiller.start();
        fulfiller.fulfill( 1 );
        fulfiller.stop();
        fulfiller.fulfill( 2 );
        fulfiller.start();
        fulfiller.fulfill( 3 );
        fulfiller.stop();
        fulfiller.fulfill( 4 );

        // Then
        verify( store1, times( 1 ) ).getLastClosedTransactionId();
        verify( store2, times( 1 ) ).getLastClosedTransactionId();
    }
}
