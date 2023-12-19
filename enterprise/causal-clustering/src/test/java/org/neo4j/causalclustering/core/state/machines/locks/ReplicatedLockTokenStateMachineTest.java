/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.state.machines.locks;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.causalclustering.core.state.storage.DurableStateStorage;
import org.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import org.neo4j.causalclustering.core.state.storage.StateMarshal;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;

public class ReplicatedLockTokenStateMachineTest
{
    @Rule
    public final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Test
    public void shouldStartWithInvalidTokenId()
    {
        // given
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine(
                new InMemoryStateStorage<>( new ReplicatedLockTokenState() ) );

        // when
        int initialTokenId = stateMachine.currentToken().id();

        // then
        assertEquals( initialTokenId, LockToken.INVALID_LOCK_TOKEN_ID );
    }

    @Test
    public void shouldIssueNextLockTokenCandidateId()
    {
        // given
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine(
                new InMemoryStateStorage<>( new ReplicatedLockTokenState() ) );
        int firstCandidateId = LockToken.nextCandidateId( stateMachine.currentToken().id() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId ), 0, r -> {} );

        // then
        assertEquals( firstCandidateId + 1, LockToken.nextCandidateId( stateMachine.currentToken().id() ) );
    }

    @Test
    public void shouldKeepTrackOfCurrentLockTokenId()
    {
        // given
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine(
                new InMemoryStateStorage<>( new ReplicatedLockTokenState() ) );
        int firstCandidateId = LockToken.nextCandidateId( stateMachine.currentToken().id() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId ), 1, r -> {} );

        // then
        assertEquals( firstCandidateId, stateMachine.currentToken().id() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId + 1 ), 2, r -> {} );

        // then
        assertEquals( firstCandidateId + 1, stateMachine.currentToken().id() );
    }

    @Test
    public void shouldKeepTrackOfLockTokenOwner()
    {
        // given
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine(
                new InMemoryStateStorage<>( new ReplicatedLockTokenState() ) );
        int firstCandidateId = LockToken.nextCandidateId( stateMachine.currentToken().id() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId ), 1, r -> {} );

        // then
        assertEquals( member( 0 ), stateMachine.currentToken().owner() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 1 ), firstCandidateId + 1 ), 2, r -> {} );

        // then
        assertEquals( member( 1 ), stateMachine.currentToken().owner() );
    }

    @Test
    public void shouldAcceptOnlyFirstRequestWithSameId()
    {
        // given
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine(
                new InMemoryStateStorage<>( new ReplicatedLockTokenState() ) );
        int firstCandidateId = LockToken.nextCandidateId( stateMachine.currentToken().id() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId ), 1, r -> {} );
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 1 ), firstCandidateId ), 2, r -> {} );

        // then
        assertEquals( 0, stateMachine.currentToken().id() );
        assertEquals( member( 0 ), stateMachine.currentToken().owner() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 1 ), firstCandidateId + 1 ), 3, r -> {} );
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId + 1 ), 4, r -> {} );

        // then
        assertEquals( 1, stateMachine.currentToken().id() );
        assertEquals( member( 1 ), stateMachine.currentToken().owner() );
    }

    @Test
    public void shouldOnlyAcceptNextImmediateId()
    {
        // given
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine(
                new InMemoryStateStorage<>( new ReplicatedLockTokenState() ) );
        int firstCandidateId = LockToken.nextCandidateId( stateMachine.currentToken().id() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId + 1 ), 1, r -> {} ); // not accepted

        // then
        assertEquals( stateMachine.currentToken().id(), LockToken.INVALID_LOCK_TOKEN_ID );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId ), 2, r -> {} ); // accepted

        // then
        assertEquals( stateMachine.currentToken().id(), firstCandidateId );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId + 1 ), 3, r -> {} ); // accepted

        // then
        assertEquals( stateMachine.currentToken().id(), firstCandidateId + 1 );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId ), 4, r -> {} ); // not accepted

        // then
        assertEquals( stateMachine.currentToken().id(), firstCandidateId + 1 );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId + 3 ), 5, r -> {} ); // not accepted

        // then
        assertEquals( stateMachine.currentToken().id(), firstCandidateId + 1 );
    }

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    @Test
    public void shouldPersistAndRecoverState() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = fileSystemRule.get();
        fsa.mkdir( testDir.directory() );

        StateMarshal<ReplicatedLockTokenState> marshal =
                new ReplicatedLockTokenState.Marshal( new MemberId.Marshal() );

        MemberId memberA = member( 0 );
        MemberId memberB = member( 1 );
        int candidateId;

        DurableStateStorage<ReplicatedLockTokenState> storage = new DurableStateStorage<>( fsa, testDir.directory(),
                "state", marshal, 100, NullLogProvider.getInstance() );
        try ( Lifespan lifespan = new Lifespan( storage ) )
        {
            ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine( storage );

            // when
            candidateId = 0;
            stateMachine.applyCommand( new ReplicatedLockTokenRequest( memberA, candidateId ), 0, r -> {} );
            candidateId = 1;
            stateMachine.applyCommand( new ReplicatedLockTokenRequest( memberB, candidateId ), 1, r -> {} );

            stateMachine.flush();
            fsa.crash();
        }

        // then
        DurableStateStorage<ReplicatedLockTokenState> storage2 = new DurableStateStorage<>(
                fsa, testDir.directory(), "state", marshal, 100, NullLogProvider.getInstance() );
        try ( Lifespan lifespan = new Lifespan( storage2 ) )
        {
            ReplicatedLockTokenState initialState = storage2.getInitialState();

            assertEquals( memberB, initialState.get().owner() );
            assertEquals( candidateId, initialState.get().id() );
        }
    }

    @Test
    public void shouldBeIdempotent()
    {
        // given
        EphemeralFileSystemAbstraction fsa = fileSystemRule.get();
        fsa.mkdir( testDir.directory() );

        StateMarshal<ReplicatedLockTokenState> marshal =
                new ReplicatedLockTokenState.Marshal( new MemberId.Marshal() );

        DurableStateStorage<ReplicatedLockTokenState> storage = new DurableStateStorage<>( fsa, testDir.directory(),
                "state", marshal, 100, NullLogProvider.getInstance() );

        try ( Lifespan lifespan = new Lifespan( storage ) )
        {
            ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine( storage );

            MemberId memberA = member( 0 );
            MemberId memberB = member( 1 );

            stateMachine.applyCommand( new ReplicatedLockTokenRequest( memberA, 0 ), 3, r ->
            {
            } );

            // when
            stateMachine.applyCommand( new ReplicatedLockTokenRequest( memberB, 1 ), 2, r ->
            {
            } );

            // then
            assertEquals( memberA, stateMachine.currentToken().owner() );
        }
    }

    @Test
    public void shouldSetInitialPendingRequestToInitialState()
    {
        // Given
        @SuppressWarnings( "unchecked" )
        StateStorage<ReplicatedLockTokenState> storage = mock( StateStorage.class );
        MemberId initialHoldingCoreMember = member( 0 );
        ReplicatedLockTokenState initialState = new ReplicatedLockTokenState( 123,
                new ReplicatedLockTokenRequest( initialHoldingCoreMember, 3 ) );
        when( storage.getInitialState() ).thenReturn( initialState );

        // When
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine( storage );

        // Then
        LockToken initialToken = stateMachine.currentToken();
        assertEquals( initialState.get().owner(), initialToken.owner() );
        assertEquals( initialState.get().id(), initialToken.id() );
    }
}
