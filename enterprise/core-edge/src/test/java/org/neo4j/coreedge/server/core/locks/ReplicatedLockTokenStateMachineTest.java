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

import org.neo4j.coreedge.raft.state.StubStateStorage;
import org.neo4j.coreedge.server.RaftTestMember;

import static org.junit.Assert.assertEquals;

import static org.neo4j.coreedge.server.RaftTestMember.member;

public class ReplicatedLockTokenStateMachineTest
{
    @Test
    public void shouldStartWithInvalidTokenId() throws Exception
    {
        // given
        LockTokenManager stateMachine = new ReplicatedLockTokenStateMachine<>(
                new StubStateStorage<>( new InMemoryReplicatedLockTokenState<RaftTestMember>() ) );

        // when
        int initialTokenId = stateMachine.currentToken().id();

        // then
        assertEquals( initialTokenId, LockToken.INVALID_LOCK_TOKEN_ID );
    }

    @Test
    public void shouldIssueNextLockTokenCandidateId() throws Exception
    {
        // given
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine<>(
                new StubStateStorage<>( new InMemoryReplicatedLockTokenState<RaftTestMember>() ) );
        int firstCandidateId = stateMachine.nextCandidateId();

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId ), 0 );

        // then
        assertEquals( firstCandidateId + 1, stateMachine.nextCandidateId() );
    }

    @Test
    public void shouldKeepTrackOfCurrentLockTokenId() throws Exception
    {
        // given
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine<>(
                new StubStateStorage<>( new InMemoryReplicatedLockTokenState<RaftTestMember>() ) );
        int firstCandidateId = stateMachine.nextCandidateId();

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId ), 1 );

        // then
        assertEquals( firstCandidateId, stateMachine.currentToken().id() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId + 1 ), 2 );

        // then
        assertEquals( firstCandidateId + 1, stateMachine.currentToken().id() );
    }

    @Test
    public void shouldKeepTrackOfLockTokenOwner() throws Exception
    {
        // given
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine<>(
                new StubStateStorage<>( new InMemoryReplicatedLockTokenState<RaftTestMember>() ) );
        int firstCandidateId = stateMachine.nextCandidateId();

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId ), 1 );

        // then
        assertEquals( member( 0 ), stateMachine.currentToken().owner() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest<>( member( 1 ), firstCandidateId + 1 ), 2 );

        // then
        assertEquals( member( 1 ), stateMachine.currentToken().owner() );
    }

    @Test
    public void shouldAcceptOnlyFirstRequestWithSameId() throws Exception
    {
        // given
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine<>(
                new StubStateStorage<>( new InMemoryReplicatedLockTokenState<RaftTestMember>() ) );
        int firstCandidateId = stateMachine.nextCandidateId();

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId ), 1 );
        stateMachine.applyCommand( new ReplicatedLockTokenRequest<>( member( 1 ), firstCandidateId ), 2 );

        // then
        assertEquals( 0, stateMachine.currentToken().id() );
        assertEquals( member( 0 ), stateMachine.currentToken().owner() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest<>( member( 1 ), firstCandidateId + 1 ), 3 );
        stateMachine.applyCommand( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId + 1 ), 4 );

        // then
        assertEquals( 1, stateMachine.currentToken().id() );
        assertEquals( member( 1 ), stateMachine.currentToken().owner() );
    }

    @Test
    public void shouldOnlyAcceptNextImmediateId() throws Exception
    {
        // given
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine<>(
                new StubStateStorage<>( new InMemoryReplicatedLockTokenState<RaftTestMember>() ) );
        int firstCandidateId = stateMachine.nextCandidateId();

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId + 1 ), 1 ); // not accepted

        // then
        assertEquals( stateMachine.currentToken().id(), LockToken.INVALID_LOCK_TOKEN_ID );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId ), 2 ); // accepted

        // then
        assertEquals( stateMachine.currentToken().id(), firstCandidateId );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId + 1 ), 3 ); // accepted

        // then
        assertEquals( stateMachine.currentToken().id(), firstCandidateId + 1 );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId ), 4 ); // not accepted

        // then
        assertEquals( stateMachine.currentToken().id(), firstCandidateId + 1 );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest<>( member( 0 ), firstCandidateId + 3 ), 5 ); // not accepted

        // then
        assertEquals( stateMachine.currentToken().id(), firstCandidateId + 1 );
    }
}
