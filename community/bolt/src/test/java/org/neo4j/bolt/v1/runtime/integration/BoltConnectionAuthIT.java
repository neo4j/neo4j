/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.BoltConnectionDescriptor;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.Version;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.testing.BoltMatchers.failedWithStatus;
import static org.neo4j.bolt.testing.BoltMatchers.succeeded;
import static org.neo4j.bolt.testing.BoltMatchers.succeededWithMetadata;
import static org.neo4j.bolt.testing.BoltMatchers.verifyKillsConnection;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.values.storable.Values.TRUE;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public class BoltConnectionAuthIT
{
    private static final String USER_AGENT = "BoltConnectionAuthIT/0.0";
    private static final BoltChannel boltChannel = mock( BoltChannel.class );

//    private static final BoltConnectionDescriptor CONNECTION_DESCRIPTOR = new BoltConnectionDescriptor(
//            new InetSocketAddress( "testClient", 56789 ),
//            new InetSocketAddress( "testServer", 7468 ) );
    @Rule
    public SessionRule env = new SessionRule().withAuthEnabled( true );

    @Test
    public void shouldGiveCredentialsExpiredStatusOnExpiredCredentials() throws Throwable
    {
        // Given it is important for client applications to programmatically
        // identify expired credentials as the cause of not being authenticated
        BoltStateMachine machine = env.newMachine( boltChannel );
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // When
        machine.init( USER_AGENT, map(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j" ), recorder );
        machine.run( "CREATE ()", EMPTY_MAP, recorder );

        // Then
        assertThat( recorder.nextResponse(), succeededWithMetadata( "credentials_expired", TRUE ) );
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Security.CredentialsExpired ) );
    }

    @Test
    public void shouldGiveKernelVersionOnInit() throws Throwable
    {
        // Given it is important for client applications to programmatically
        // identify expired credentials as the cause of not being authenticated
//<<<<<<< 24278c4de3ee849106c96df999c3269a90db8c73
//        BoltStateMachine machine = env.newMachine( CONNECTION_DESCRIPTOR );
//=======
        BoltStateMachine machine = env.newMachine( boltChannel );
//>>>>>>> Bolt message logging
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        String version = "Neo4j/" + Version.getNeo4jVersion();
        // When
        machine.init( USER_AGENT, map(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j" ), recorder );
        machine.run( "CREATE ()", EMPTY_MAP, recorder );

        // Then
        assertThat( recorder.nextResponse(), succeededWithMetadata( "server", stringValue( version ) ) );
    }

    @Test
    public void shouldCloseConnectionAfterAuthenticationFailure() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );

        // When... then
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        verifyKillsConnection( () -> machine.init( USER_AGENT, map(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "j4oen"
        ), recorder ) );

        // ...and
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Security.Unauthorized ) );
    }

    @Test
    public void shouldBeAbleToActOnSessionWhenUpdatingCredentials() throws Throwable
    {
        BoltStateMachine machine = env.newMachine( boltChannel );
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // when
        machine.init( USER_AGENT, map(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j",
                "new_credentials", "secret"
        ), recorder );
        machine.run( "CREATE ()", EMPTY_MAP, recorder );

        // then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
    }
}
