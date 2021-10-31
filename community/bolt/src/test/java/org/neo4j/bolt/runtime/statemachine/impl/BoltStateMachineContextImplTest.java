/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.runtime.statemachine.impl;

import io.netty.channel.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineSPI;
import org.neo4j.bolt.runtime.statemachine.MutableConnectionState;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.memory.MemoryTracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.testing.BoltTestUtil.newTestBoltChannel;

class BoltStateMachineContextImplTest
{
    private BoltChannel channel;
    private BoltStateMachine machine;
    private BoltStateMachineSPI stateMachineSPI;
    private MutableConnectionState connectionState;
    private DefaultDatabaseResolver databaseResolver;
    private MemoryTracker memoryTracker;
    private TransactionManager transactionManager;

    private BoltStateMachineContextImpl context;

    @BeforeEach
    void prepareContext()
    {
        this.channel = spy( newTestBoltChannel( mock( Channel.class ) ) );
        this.machine = mock( BoltStateMachine.class );
        this.stateMachineSPI = mock( BoltStateMachineSPI.class );
        this.connectionState = new MutableConnectionState();
        this.databaseResolver = mock( DefaultDatabaseResolver.class );
        this.memoryTracker = mock( MemoryTracker.class, RETURNS_MOCKS );
        this.transactionManager = mock( TransactionManager.class );

        this.context = new BoltStateMachineContextImpl( this.machine, this.channel, this.stateMachineSPI, this.connectionState, Clock.systemUTC(),
                                                        this.databaseResolver, this.memoryTracker, this.transactionManager );
    }

    @Test
    void shouldHandleFailure() throws BoltConnectionFatality
    {
        RuntimeException cause = new RuntimeException();
        context.handleFailure( cause, true );

        verify( machine ).handleFailure( cause, true );
    }

    @Test
    void shouldResetMachine() throws BoltConnectionFatality
    {
        context.resetMachine();

        verify( machine ).reset();
    }

    @Test
    void releaseShouldResetTransactionState() throws Throwable
    {
        assertSame( connectionState, context.connectionState() );

        context.connectionState().setCurrentTransactionId( "123" );

        // When
        context.releaseStatementProcessor( "123" );

        // Then
        assertNull( context.connectionState().getCurrentTransactionId() );
    }

    @Test
    void impersonationShouldResolveHomeDatabase()
    {
        when( this.databaseResolver.defaultDatabase( "bob" ) ).thenReturn( "neo4j" );
        when( this.databaseResolver.defaultDatabase( "grace" ) ).thenReturn( "secretdb" );

        var subject = mock( AuthSubject.class );
        when( subject.authenticatedUser() ).thenReturn( "bob" );
        when( subject.executingUser() ).thenReturn( "bob" );

        var loginContext = mock( LoginContext.class );
        when( loginContext.subject() ).thenReturn( subject );

        context.authenticatedAsUser( loginContext, "Test/0.0.0" );

        verify( this.databaseResolver ).defaultDatabase( "bob" );
        verify( this.channel ).updateDefaultDatabase( "neo4j" );
        verify( this.channel ).updateUser( "bob", "Test/0.0.0" );

        assertThat( this.context.getLoginContext() ).isSameAs( loginContext );
        assertThat( this.context.getDefaultDatabase() ).isEqualTo( "neo4j" );

        var impersonatedSubject = mock( AuthSubject.class );
        when( impersonatedSubject.authenticatedUser() ).thenReturn( "bob" );
        when( impersonatedSubject.executingUser() ).thenReturn( "grace" );

        var impersonatedLoginContext = mock( LoginContext.class );
        when( impersonatedLoginContext.subject() ).thenReturn( impersonatedSubject );

        context.impersonateUser( impersonatedLoginContext );

        verify( this.databaseResolver ).defaultDatabase( "grace" );
        verify( this.channel ).updateDefaultDatabase( "secretdb" );
        verifyNoMoreInteractions( this.databaseResolver );

        assertThat( this.context.getLoginContext() ).isSameAs( impersonatedLoginContext );
        assertThat( this.context.getDefaultDatabase() ).isEqualTo( "secretdb" );
    }
}
