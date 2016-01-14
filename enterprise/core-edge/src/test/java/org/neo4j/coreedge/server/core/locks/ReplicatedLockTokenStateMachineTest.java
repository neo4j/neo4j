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
package org.neo4j.coreedge.server.core.locks;

import org.junit.Test;

import org.neo4j.coreedge.raft.replication.StubReplicator;

import static org.junit.Assert.assertEquals;

import static org.neo4j.coreedge.server.RaftTestMember.member;

public class ReplicatedLockTokenStateMachineTest
{
    @Test
    public void shouldStartWithInvalidTokenId() throws Exception
    {
        // given
        StubReplicator replicator = new StubReplicator();
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine<>( replicator );

        // when
        int initialTokenId = stateMachine.currentToken().id();

        // then
        assertEquals( initialTokenId, LockToken.INVALID_LOCK_TOKEN_ID );
    }

    @Test
    public void shouldIssueNextLockTokenCandidateId() throws Exception
    {
        // given
        StubReplicator replicator = new StubReplicator();
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine<>( replicator );
        int firstCandidateId = stateMachine.nextCandidateId();

        // when
        replicator.replicate( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId ) );

        // then
        assertEquals( firstCandidateId+1, stateMachine.nextCandidateId() );
    }

    @Test
    public void shouldKeepTrackOfCurrentLockTokenId() throws Exception
    {
        // given
        StubReplicator replicator = new StubReplicator();
        LockTokenManager stateMachine = new ReplicatedLockTokenStateMachine<>( replicator );
        int firstCandidateId = stateMachine.nextCandidateId();

        // when
        replicator.replicate( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId ) );

        // then
        assertEquals( firstCandidateId, stateMachine.currentToken().id() );

        // when
        replicator.replicate( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId+1 ) );

        // then
        assertEquals( firstCandidateId+1, stateMachine.currentToken().id() );
    }

    @Test
    public void shouldKeepTrackOfLockTokenOwner() throws Exception
    {
        // given
        StubReplicator replicator = new StubReplicator();
        LockTokenManager stateMachine = new ReplicatedLockTokenStateMachine<>( replicator );
        int firstCandidateId = stateMachine.nextCandidateId();

        // when
        replicator.replicate( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId ) );

        // then
        assertEquals( member( 0 ), stateMachine.currentToken().owner() );

        // when
        replicator.replicate( new ReplicatedLockTokenRequest<>( member( 1 ), firstCandidateId+1 ) );

        // then
        assertEquals( member( 1 ), stateMachine.currentToken().owner() );
    }

    @Test
    public void shouldAcceptOnlyFirstRequestWithSameId() throws Exception
    {
        // given
        StubReplicator replicator = new StubReplicator();
        LockTokenManager stateMachine = new ReplicatedLockTokenStateMachine<>( replicator );
        int firstCandidateId = stateMachine.nextCandidateId();

        // when
        replicator.replicate( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId ) );
        replicator.replicate( new ReplicatedLockTokenRequest<>( member( 1 ), firstCandidateId ) );

        // then
        assertEquals( 0, stateMachine.currentToken().id() );
        assertEquals( member( 0 ), stateMachine.currentToken().owner() );

        // when
        replicator.replicate( new ReplicatedLockTokenRequest<>( member( 1 ), firstCandidateId+1 ) );
        replicator.replicate( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId+1 ) );

        // then
        assertEquals( 1, stateMachine.currentToken().id() );
        assertEquals( member( 1 ), stateMachine.currentToken().owner() );
    }

    @Test
    public void shouldOnlyAcceptNextImmediateId() throws Exception
    {
        // given
        StubReplicator replicator = new StubReplicator();
        LockTokenManager stateMachine = new ReplicatedLockTokenStateMachine<>( replicator );
        int firstCandidateId = stateMachine.nextCandidateId();

        // when
        replicator.replicate( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId+1 ) ); // not accepted

        // then
        assertEquals( stateMachine.currentToken().id(), LockToken.INVALID_LOCK_TOKEN_ID );

        // when
        replicator.replicate( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId ) ); // accepted

        // then
        assertEquals( stateMachine.currentToken().id(), firstCandidateId );

        // when
        replicator.replicate( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId+1 ) ); // accepted

        // then
        assertEquals( stateMachine.currentToken().id(), firstCandidateId+1 );

        // when
        replicator.replicate( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId ) ); // not accepted

        // then
        assertEquals( stateMachine.currentToken().id(), firstCandidateId+1 );

        // when
        replicator.replicate( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId+3 ) ); // not accepted

        // then
        assertEquals( stateMachine.currentToken().id(), firstCandidateId+1 );
    }
}
