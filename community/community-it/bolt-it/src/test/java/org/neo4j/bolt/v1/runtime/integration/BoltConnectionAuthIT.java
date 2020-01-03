/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.v1.runtime.integration;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.testing.BoltTestUtil;
import org.neo4j.bolt.v1.messaging.request.InitMessage;
import org.neo4j.bolt.v1.messaging.request.RunMessage;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.Version;
import org.neo4j.string.UTF8;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.testing.BoltMatchers.failedWithStatus;
import static org.neo4j.bolt.testing.BoltMatchers.succeeded;
import static org.neo4j.bolt.testing.BoltMatchers.succeededWithMetadata;
import static org.neo4j.bolt.testing.BoltMatchers.verifyKillsConnection;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.security.AuthToken.newBasicAuthToken;
import static org.neo4j.values.storable.Values.TRUE;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public class BoltConnectionAuthIT
{
    private static final String USER_AGENT = "BoltConnectionAuthIT/0.0";
    private static final BoltChannel BOLT_CHANNEL = BoltTestUtil.newTestBoltChannel();

    @Rule
    public SessionRule env = new SessionRule().withAuthEnabled( true );

    @Test
    public void shouldGiveCredentialsExpiredStatusOnExpiredCredentials() throws Throwable
    {
        // Given it is important for client applications to programmatically
        // identify expired credentials as the cause of not being authenticated
        BoltStateMachine machine = env.newMachine( BOLT_CHANNEL );
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // When
        InitMessage init = new InitMessage( USER_AGENT, newBasicAuthToken( "neo4j", "neo4j" ) );

        machine.process( init, recorder );
        machine.process( new RunMessage( "CREATE ()", EMPTY_MAP ), recorder );

        // Then
        assertThat( recorder.nextResponse(), succeededWithMetadata( "credentials_expired", TRUE ) );
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Security.CredentialsExpired ) );
    }

    @Test
    public void shouldGiveKernelVersionOnInit() throws Throwable
    {
        // Given it is important for client applications to programmatically
        // identify expired credentials as the cause of not being authenticated
        BoltStateMachine machine = env.newMachine( BOLT_CHANNEL );
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        String version = "Neo4j/" + Version.getNeo4jVersion();

        // When
        InitMessage init = new InitMessage( USER_AGENT, newBasicAuthToken( "neo4j", "neo4j" ) );

        machine.process( init, recorder );
        machine.process( new RunMessage( "CREATE ()", EMPTY_MAP ), recorder );

        // Then
        assertThat( recorder.nextResponse(), succeededWithMetadata( "server", stringValue( version ) ) );
    }

    @Test
    public void shouldCloseConnectionAfterAuthenticationFailure() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( BOLT_CHANNEL );

        // When... then
        InitMessage init = new InitMessage( USER_AGENT, newBasicAuthToken( "neo4j", "j4oen" ) );
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        verifyKillsConnection( () -> machine.process( init, recorder ) );

        // ...and
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Security.Unauthorized ) );
    }

    @Test
    public void shouldBeAbleToActOnSessionWhenUpdatingCredentials() throws Throwable
    {
        BoltStateMachine machine = env.newMachine( BOLT_CHANNEL );
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // when
        InitMessage message = new InitMessage( USER_AGENT, map(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", UTF8.encode( "neo4j" ),
                "new_credentials", UTF8.encode( "secret" ) ) );
        machine.process( message, recorder );
        machine.process( new RunMessage( "CREATE ()", EMPTY_MAP ), recorder );

        // then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
    }
}
