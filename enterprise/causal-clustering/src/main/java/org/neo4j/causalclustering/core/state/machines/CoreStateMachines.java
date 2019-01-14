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

import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyMachine;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import org.neo4j.causalclustering.core.state.machines.tx.RecoverConsensusLogIndex;
import org.neo4j.causalclustering.core.state.snapshot.CoreStateType;
import org.neo4j.causalclustering.core.state.CommandDispatcher;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationStateMachine;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenStateMachine;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionStateMachine;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenStateMachine;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.storageengine.api.Token;

import static java.lang.Math.max;

public class CoreStateMachines
{
    private final ReplicatedTransactionStateMachine replicatedTxStateMachine;

    private final ReplicatedTokenStateMachine<Token> labelTokenStateMachine;
    private final ReplicatedTokenStateMachine<RelationshipTypeToken> relationshipTypeTokenStateMachine;
    private final ReplicatedTokenStateMachine<Token> propertyKeyTokenStateMachine;

    private final ReplicatedLockTokenStateMachine replicatedLockTokenStateMachine;
    private final ReplicatedIdAllocationStateMachine idAllocationStateMachine;

    private final DummyMachine benchmarkMachine;

    private final LocalDatabase localDatabase;
    private final RecoverConsensusLogIndex consensusLogIndexRecovery;

    private final CommandDispatcher dispatcher = new StateMachineCommandDispatcher();
    private volatile boolean runningBatch;

    CoreStateMachines(
            ReplicatedTransactionStateMachine replicatedTxStateMachine,
            ReplicatedTokenStateMachine<Token> labelTokenStateMachine,
            ReplicatedTokenStateMachine<RelationshipTypeToken> relationshipTypeTokenStateMachine,
            ReplicatedTokenStateMachine<Token> propertyKeyTokenStateMachine,
            ReplicatedLockTokenStateMachine replicatedLockTokenStateMachine,
            ReplicatedIdAllocationStateMachine idAllocationStateMachine,
            DummyMachine benchmarkMachine,
            LocalDatabase localDatabase,
            RecoverConsensusLogIndex consensusLogIndexRecovery )
    {
        this.replicatedTxStateMachine = replicatedTxStateMachine;
        this.labelTokenStateMachine = labelTokenStateMachine;
        this.relationshipTypeTokenStateMachine = relationshipTypeTokenStateMachine;
        this.propertyKeyTokenStateMachine = propertyKeyTokenStateMachine;
        this.replicatedLockTokenStateMachine = replicatedLockTokenStateMachine;
        this.idAllocationStateMachine = idAllocationStateMachine;
        this.benchmarkMachine = benchmarkMachine;
        this.localDatabase = localDatabase;
        this.consensusLogIndexRecovery = consensusLogIndexRecovery;
    }

    public CommandDispatcher commandDispatcher()
    {
        localDatabase.assertHealthy( IllegalStateException.class );
        assert !runningBatch;
        runningBatch = true;
        return dispatcher;
    }

    public long getLastAppliedIndex()
    {
        long lastAppliedLockTokenIndex = replicatedLockTokenStateMachine.lastAppliedIndex();
        long lastAppliedIdAllocationIndex = idAllocationStateMachine.lastAppliedIndex();
        return max( lastAppliedLockTokenIndex, lastAppliedIdAllocationIndex );
    }

    public void flush() throws IOException
    {
        assert !runningBatch;

        replicatedTxStateMachine.flush();

        labelTokenStateMachine.flush();
        relationshipTypeTokenStateMachine.flush();
        propertyKeyTokenStateMachine.flush();

        replicatedLockTokenStateMachine.flush();
        idAllocationStateMachine.flush();
    }

    public void addSnapshots( CoreSnapshot coreSnapshot )
    {
        assert !runningBatch;

        coreSnapshot.add( CoreStateType.ID_ALLOCATION, idAllocationStateMachine.snapshot() );
        coreSnapshot.add( CoreStateType.LOCK_TOKEN, replicatedLockTokenStateMachine.snapshot() );
        // transactions and tokens live in the store
    }

    public void installSnapshots( CoreSnapshot coreSnapshot )
    {
        assert !runningBatch;

        idAllocationStateMachine.installSnapshot( coreSnapshot.get( CoreStateType.ID_ALLOCATION ) );
        replicatedLockTokenStateMachine.installSnapshot( coreSnapshot.get( CoreStateType.LOCK_TOKEN ) );
        // transactions and tokens live in the store
    }

    public void installCommitProcess( TransactionCommitProcess localCommit )
    {
        assert !runningBatch;
        long lastAppliedIndex = consensusLogIndexRecovery.findLastAppliedIndex();

        replicatedTxStateMachine.installCommitProcess( localCommit, lastAppliedIndex );

        labelTokenStateMachine.installCommitProcess( localCommit, lastAppliedIndex );
        relationshipTypeTokenStateMachine.installCommitProcess( localCommit, lastAppliedIndex );
        propertyKeyTokenStateMachine.installCommitProcess( localCommit, lastAppliedIndex );
    }

    private class StateMachineCommandDispatcher implements CommandDispatcher
    {
        @Override
        public void dispatch( ReplicatedTransaction transaction, long commandIndex, Consumer<Result> callback )
        {
            replicatedTxStateMachine.applyCommand( transaction, commandIndex, callback );
        }

        @Override
        public void dispatch( ReplicatedIdAllocationRequest idRequest, long commandIndex, Consumer<Result> callback )
        {
            replicatedTxStateMachine.ensuredApplied();
            idAllocationStateMachine.applyCommand( idRequest, commandIndex, callback );
        }

        @Override
        public void dispatch( ReplicatedTokenRequest tokenRequest, long commandIndex, Consumer<Result> callback )
        {
            replicatedTxStateMachine.ensuredApplied();
            switch ( tokenRequest.type() )
            {
            case PROPERTY:
                propertyKeyTokenStateMachine.applyCommand( tokenRequest, commandIndex, callback );
                break;
            case RELATIONSHIP:
                relationshipTypeTokenStateMachine.applyCommand( tokenRequest, commandIndex, callback );
                break;
            case LABEL:
                labelTokenStateMachine.applyCommand( tokenRequest, commandIndex, callback );
                break;
            default:
                throw new IllegalStateException();
            }
        }

        @Override
        public void dispatch( ReplicatedLockTokenRequest lockRequest, long commandIndex, Consumer<Result> callback )
        {
            replicatedTxStateMachine.ensuredApplied();
            replicatedLockTokenStateMachine.applyCommand( lockRequest, commandIndex, callback );
        }

        @Override
        public void dispatch( DummyRequest dummyRequest, long commandIndex, Consumer<Result> callback )
        {
            benchmarkMachine.applyCommand( dummyRequest, commandIndex, callback );
        }

        @Override
        public void close()
        {
            runningBatch = false;
            replicatedTxStateMachine.ensuredApplied();
        }
    }
}
