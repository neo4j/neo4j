/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.state.machines;

import org.junit.Test;
import org.mockito.InOrder;

import java.util.function.Consumer;

import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.core.state.CommandDispatcher;
import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyMachine;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationStateMachine;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenStateMachine;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenStateMachine;
import org.neo4j.causalclustering.core.state.machines.token.TokenType;
import org.neo4j.causalclustering.core.state.machines.tx.RecoverConsensusLogIndex;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionStateMachine;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.storageengine.api.Token;

import static java.lang.Math.max;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoreStateMachinesTest
{
    @Test
    public void shouldAllowForBatchingOfTransactions()
    {
        try ( CommandDispatcher dispatcher = coreStateMachines.commandDispatcher() )
        {
            dispatcher.dispatch( replicatedTransaction, 0, callback );
            dispatcher.dispatch( replicatedTransaction, 1, callback );
            dispatcher.dispatch( replicatedTransaction, 2, callback );
        }

        verifier.verify( txSM ).applyCommand( replicatedTransaction, 0, callback );
        verifier.verify( txSM ).applyCommand( replicatedTransaction, 1, callback );
        verifier.verify( txSM ).applyCommand( replicatedTransaction, 2, callback );
        verifier.verify( txSM ).ensuredApplied();
        verifier.verifyNoMoreInteractions();
    }

    @Test
    public void shouldApplyTransactionBatchAsSoonAsThereIsADifferentKindOfRequestInTheBatch()
    {
        try ( CommandDispatcher dispatcher = coreStateMachines.commandDispatcher() )
        {
            dispatcher.dispatch( replicatedTransaction, 0, callback );
            dispatcher.dispatch( replicatedTransaction, 1, callback );

            dispatcher.dispatch( iAllocationRequest, 2, callback  );

            dispatcher.dispatch( replicatedTransaction, 3, callback );
            dispatcher.dispatch( replicatedTransaction, 4, callback );

            dispatcher.dispatch( relationshipTypeTokenRequest, 5, callback  );

            dispatcher.dispatch( replicatedTransaction, 6, callback );
            dispatcher.dispatch( replicatedTransaction, 7, callback );

            dispatcher.dispatch( lockTokenRequest, 8, callback );

            dispatcher.dispatch( replicatedTransaction, 9, callback );
            dispatcher.dispatch( replicatedTransaction, 10, callback );
        }

        verifier.verify( txSM ).applyCommand( replicatedTransaction, 0, callback );
        verifier.verify( txSM ).applyCommand( replicatedTransaction, 1, callback );
        verifier.verify( txSM ).ensuredApplied();

        verifier.verify( idAllocationSM ).applyCommand( iAllocationRequest, 2, callback );

        verifier.verify( txSM ).applyCommand( replicatedTransaction, 3, callback );
        verifier.verify( txSM ).applyCommand( replicatedTransaction, 4, callback );
        verifier.verify( txSM ).ensuredApplied();

        verifier.verify( relationshipTypeTokenSM ).applyCommand( relationshipTypeTokenRequest, 5, callback );

        verifier.verify( txSM ).applyCommand( replicatedTransaction, 6, callback );
        verifier.verify( txSM ).applyCommand( replicatedTransaction, 7, callback );
        verifier.verify( txSM ).ensuredApplied();

        verifier.verify( lockTokenSM ).applyCommand( lockTokenRequest, 8, callback );

        verifier.verify( txSM ).applyCommand( replicatedTransaction, 9, callback );
        verifier.verify( txSM ).applyCommand( replicatedTransaction, 10, callback );
        verifier.verify( txSM ).ensuredApplied();

        verifier.verifyNoMoreInteractions();
    }

    @Test
    public void shouldReturnLastAppliedOfAllStateMachines()
    {
        // tx state machines are backed by the same store (the tx log) so they should return the same lastApplied
        StateMachine<?>[] txSMs = new StateMachine[]{labelTokenSM, relationshipTypeTokenSM, propertyKeyTokenSM, txSM};

        // these have separate storage
        StateMachine<?>[] otherSMs = new StateMachine[]{idAllocationSM, lockTokenSM};

        int totalDistinctSMs = otherSMs.length + 1; // distinct meaning backed by different storage
        // here we try to order all the distinct state machines in different orders to prove that,
        // regardless of which one is latest, we still recover the latest applied index
        for ( long base = 0; base < totalDistinctSMs; base++ )
        {
            long expected = -1;
            long index = 0;
            long lastAppliedIndex;
            for ( StateMachine<?> sm : otherSMs )
            {
                lastAppliedIndex = (base + index) % totalDistinctSMs;
                expected = max( expected, lastAppliedIndex ); // this means that result is ignoring the txSMs
                when( sm.lastAppliedIndex() ).thenReturn( lastAppliedIndex );
                index++;
            }

            lastAppliedIndex = (base + index) % totalDistinctSMs; // all the txSMs have the same backing store
            for ( StateMachine<?> sm : txSMs )
            {
                when( sm.lastAppliedIndex() ).thenReturn( lastAppliedIndex );
            }

            // then
            assertEquals( expected, coreStateMachines.getLastAppliedIndex() );
        }
    }

    @SuppressWarnings( "unchecked" )
    private final ReplicatedTransactionStateMachine txSM = mock( ReplicatedTransactionStateMachine.class );
    @SuppressWarnings( "unchecked" )
    private final ReplicatedTokenStateMachine<Token> labelTokenSM = mock( ReplicatedTokenStateMachine.class );
    @SuppressWarnings( "unchecked" )
    private final ReplicatedTokenStateMachine<RelationshipTypeToken> relationshipTypeTokenSM =
            mock( ReplicatedTokenStateMachine.class );
    @SuppressWarnings( "unchecked" )
    private final ReplicatedTokenStateMachine<Token> propertyKeyTokenSM = mock( ReplicatedTokenStateMachine.class );
    @SuppressWarnings( "unchecked" )
    private final ReplicatedLockTokenStateMachine lockTokenSM =
            mock( ReplicatedLockTokenStateMachine.class );
    @SuppressWarnings( "unchecked" )
    private final ReplicatedIdAllocationStateMachine idAllocationSM = mock( ReplicatedIdAllocationStateMachine.class );
    private final DummyMachine dummySM = mock( DummyMachine.class );
    private final RecoverConsensusLogIndex recoverConsensusLogIndex = mock( RecoverConsensusLogIndex.class );

    private final CoreStateMachines coreStateMachines = new CoreStateMachines( txSM, labelTokenSM,
            relationshipTypeTokenSM, propertyKeyTokenSM, lockTokenSM, idAllocationSM, dummySM,
            mock( LocalDatabase.class ), recoverConsensusLogIndex );

    private final ReplicatedTransaction replicatedTransaction = mock( ReplicatedTransaction.class );
    private final ReplicatedIdAllocationRequest iAllocationRequest = mock( ReplicatedIdAllocationRequest.class );
    private final ReplicatedTokenRequest relationshipTypeTokenRequest = mock( ReplicatedTokenRequest.class );
    {
        when( relationshipTypeTokenRequest.type() ).thenReturn( TokenType.RELATIONSHIP );
    }

    @SuppressWarnings( "unchecked" )
    private final ReplicatedLockTokenRequest lockTokenRequest = mock( ReplicatedLockTokenRequest.class );

    @SuppressWarnings( "unchecked" )
    private final Consumer<Result> callback = mock( Consumer.class );

    private final InOrder verifier =
            inOrder( txSM, labelTokenSM, relationshipTypeTokenSM, propertyKeyTokenSM, lockTokenSM, idAllocationSM );
}
