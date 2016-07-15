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
package org.neo4j.coreedge.raft.state;

import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.core.CoreStateType;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdAllocationRequest;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdAllocationStateMachine;
import org.neo4j.coreedge.raft.replication.token.ReplicatedTokenRequest;
import org.neo4j.coreedge.raft.replication.token.ReplicatedTokenStateMachine;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransaction;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionStateMachine;
import org.neo4j.coreedge.server.core.RecoverTransactionLogState;
import org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenRequest;
import org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenStateMachine;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
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
    private final CoreState coreState;
    private final RecoverTransactionLogState txLogState;
    private final RaftLog raftLog;
    private final LocalDatabase localDatabase;

    private final CommandDispatcher currentBatch = new StateMachineCommandDispatcher();
    private volatile boolean runningBatch;

    public CoreStateMachines(
            ReplicatedTransactionStateMachine replicatedTxStateMachine,
            ReplicatedTokenStateMachine<Token> labelTokenStateMachine,
            ReplicatedTokenStateMachine<RelationshipTypeToken> relationshipTypeTokenStateMachine,
            ReplicatedTokenStateMachine<Token> propertyKeyTokenStateMachine,
            ReplicatedLockTokenStateMachine replicatedLockTokenStateMachine,
            ReplicatedIdAllocationStateMachine idAllocationStateMachine,
            CoreState coreState,
            RecoverTransactionLogState txLogState,
            RaftLog raftLog,
            LocalDatabase localDatabase )
    {
        this.replicatedTxStateMachine = replicatedTxStateMachine;
        this.labelTokenStateMachine = labelTokenStateMachine;
        this.relationshipTypeTokenStateMachine = relationshipTypeTokenStateMachine;
        this.propertyKeyTokenStateMachine = propertyKeyTokenStateMachine;
        this.replicatedLockTokenStateMachine = replicatedLockTokenStateMachine;
        this.idAllocationStateMachine = idAllocationStateMachine;
        this.coreState = coreState;
        this.txLogState = txLogState;
        this.raftLog = raftLog;
        this.localDatabase = localDatabase;
    }

    CommandDispatcher commandDispatcher()
    {
        localDatabase.assertHealthy( IllegalStateException.class );
        assert !runningBatch;
        runningBatch = true;
        return currentBatch;
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

    void addSnapshots( CoreSnapshot coreSnapshot )
    {
        assert !runningBatch;

        coreSnapshot.add( CoreStateType.ID_ALLOCATION, idAllocationStateMachine.snapshot() );
        coreSnapshot.add( CoreStateType.LOCK_TOKEN, replicatedLockTokenStateMachine.snapshot() );
        // transactions and tokens live in the store
    }

    void installSnapshots( CoreSnapshot coreSnapshot )
    {
        assert !runningBatch;

        idAllocationStateMachine.installSnapshot( coreSnapshot.get( CoreStateType.ID_ALLOCATION ) );
        replicatedLockTokenStateMachine.installSnapshot( coreSnapshot.get( CoreStateType.LOCK_TOKEN ) );
        // transactions and tokens live in the store

        long snapshotPrevIndex = coreSnapshot.prevIndex();
        try
        {
            if ( snapshotPrevIndex > 1 )
            {
                raftLog.skip( snapshotPrevIndex, coreSnapshot.prevTerm() );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        coreState.skip( snapshotPrevIndex );
    }

    public void refresh( TransactionRepresentationCommitProcess localCommit )
    {
        assert !runningBatch;

        long lastAppliedIndex = txLogState.findLastAppliedIndex();
        replicatedTxStateMachine.installCommitProcess( localCommit, lastAppliedIndex );

        labelTokenStateMachine.installCommitProcess( localCommit, lastAppliedIndex );
        relationshipTypeTokenStateMachine.installCommitProcess( localCommit, lastAppliedIndex );
        propertyKeyTokenStateMachine.installCommitProcess( localCommit, lastAppliedIndex );

        coreState.setStateMachine( this );
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
        public void close()
        {
            runningBatch = false;
            replicatedTxStateMachine.ensuredApplied();
        }
    }

    long getLastAppliedIndex()
    {
        long lastAppliedTxIndex = replicatedTxStateMachine.lastAppliedIndex();
        assert lastAppliedTxIndex == labelTokenStateMachine.lastAppliedIndex();
        assert lastAppliedTxIndex == relationshipTypeTokenStateMachine.lastAppliedIndex();
        assert lastAppliedTxIndex == propertyKeyTokenStateMachine.lastAppliedIndex();

        long lastAppliedLockTokenIndex = replicatedLockTokenStateMachine.lastAppliedIndex();
        long lastAppliedIdAllocationIndex = idAllocationStateMachine.lastAppliedIndex();

        return max( max( lastAppliedLockTokenIndex, lastAppliedIdAllocationIndex ), lastAppliedTxIndex );
    }
}
