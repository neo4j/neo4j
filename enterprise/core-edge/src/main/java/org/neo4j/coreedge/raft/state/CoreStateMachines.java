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
import java.util.Optional;

import org.neo4j.coreedge.catchup.storecopy.core.CoreStateType;
import org.neo4j.coreedge.raft.log.MonitoredRaftLog;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdAllocationRequest;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdAllocationStateMachine;
import org.neo4j.coreedge.raft.replication.token.ReplicatedTokenRequest;
import org.neo4j.coreedge.raft.replication.token.ReplicatedTokenStateMachine;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransaction;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionStateMachine;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.core.RecoverTransactionLogState;
import org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenRequest;
import org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenStateMachine;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.storageengine.api.Token;

public class CoreStateMachines
{
    private final ReplicatedTransactionStateMachine<CoreMember> replicatedTxStateMachine;

    private final ReplicatedTokenStateMachine<Token> labelTokenStateMachine;
    private final ReplicatedTokenStateMachine<RelationshipTypeToken> relationshipTypeTokenStateMachine;
    private final ReplicatedTokenStateMachine<Token> propertyKeyTokenStateMachine;

    private final ReplicatedLockTokenStateMachine<CoreMember> replicatedLockTokenStateMachine;
    private final ReplicatedIdAllocationStateMachine idAllocationStateMachine;
    private final CoreState coreState;
    private final RecoverTransactionLogState txLogState;
    private final MonitoredRaftLog raftLog;

    public CoreStateMachines(
            ReplicatedTransactionStateMachine<CoreMember> replicatedTxStateMachine,
            ReplicatedTokenStateMachine<Token> labelTokenStateMachine,
            ReplicatedTokenStateMachine<RelationshipTypeToken> relationshipTypeTokenStateMachine,
            ReplicatedTokenStateMachine<Token> propertyKeyTokenStateMachine,
            ReplicatedLockTokenStateMachine<CoreMember> replicatedLockTokenStateMachine,
            ReplicatedIdAllocationStateMachine idAllocationStateMachine,
            CoreState coreState,
            RecoverTransactionLogState txLogState,
            MonitoredRaftLog raftLog )
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
    }

    public Optional<Result> dispatch( ReplicatedTransaction transaction, long commandIndex )
    {
        return replicatedTxStateMachine.applyCommand( transaction, commandIndex );
    }

    public Optional<Result> dispatch( ReplicatedIdAllocationRequest idRequest, long commandIndex )
    {
        return idAllocationStateMachine.applyCommand( idRequest, commandIndex );
    }

    public Optional<Result> dispatch( ReplicatedTokenRequest tokenRequest, long commandIndex )
    {
        switch ( tokenRequest.type() )
        {
        case PROPERTY:
            return propertyKeyTokenStateMachine.applyCommand( tokenRequest, commandIndex );
        case RELATIONSHIP:
            return relationshipTypeTokenStateMachine.applyCommand( tokenRequest, commandIndex );
        case LABEL:
            return labelTokenStateMachine.applyCommand( tokenRequest, commandIndex );
        default:
            throw new IllegalStateException();
        }
    }

    public Optional<Result> dispatch( ReplicatedLockTokenRequest lockRequest, long commandIndex )
    {
        return replicatedLockTokenStateMachine.applyCommand( lockRequest, commandIndex );
    }

    public void flush() throws IOException
    {
        replicatedTxStateMachine.flush();

        labelTokenStateMachine.flush();
        relationshipTypeTokenStateMachine.flush();
        propertyKeyTokenStateMachine.flush();

        replicatedLockTokenStateMachine.flush();
        idAllocationStateMachine.flush();
    }

    public void addSnapshots( CoreSnapshot coreSnapshot )
    {
        coreSnapshot.add( CoreStateType.ID_ALLOCATION, idAllocationStateMachine.snapshot() );
        coreSnapshot.add( CoreStateType.LOCK_TOKEN, replicatedLockTokenStateMachine.snapshot() );
        // transactions and tokens live in the store
    }

    void installSnapshots( CoreSnapshot coreSnapshot )
    {
        idAllocationStateMachine.installSnapshot( coreSnapshot.get( CoreStateType.ID_ALLOCATION ) );
        replicatedLockTokenStateMachine.installSnapshot( coreSnapshot.get( CoreStateType.LOCK_TOKEN ) );
        // transactions and tokens live in the store

        long snapshotPrevIndex = coreSnapshot.prevIndex();
        try
        {
            if( snapshotPrevIndex > 1)
            {
                raftLog.skip( snapshotPrevIndex, coreSnapshot.prevTerm() );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void refresh( TransactionRepresentationCommitProcess localCommit )
    {
        long lastAppliedIndex = txLogState.findLastAppliedIndex();
        replicatedTxStateMachine.installCommitProcess( localCommit, lastAppliedIndex );

        labelTokenStateMachine.installCommitProcess( localCommit, lastAppliedIndex );
        relationshipTypeTokenStateMachine.installCommitProcess( localCommit, lastAppliedIndex );
        propertyKeyTokenStateMachine.installCommitProcess( localCommit, lastAppliedIndex );

        coreState.setStateMachine( this, lastAppliedIndex );
    }
}
