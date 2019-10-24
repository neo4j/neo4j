/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.testing.BoltTestUtil;
import org.neo4j.bolt.v4.BoltProtocolV4;
import org.neo4j.bolt.v4.BoltStateMachineV4;
import org.neo4j.bolt.v4.messaging.BoltV4Messages;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.Version;
import org.neo4j.string.UTF8;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.testing.BoltMatchers.failedWithStatus;
import static org.neo4j.bolt.testing.BoltMatchers.succeeded;
import static org.neo4j.bolt.testing.BoltMatchers.succeededWithMetadata;
import static org.neo4j.bolt.testing.BoltMatchers.verifyKillsConnection;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.security.AuthToken.newBasicAuthToken;
import static org.neo4j.values.storable.Values.TRUE;
import static org.neo4j.values.storable.Values.stringValue;

class BoltConnectionAuthIT
{
    private static final BoltChannel BOLT_CHANNEL = BoltTestUtil.newTestBoltChannel( "conn-v4-test-boltchannel-id" );

    @RegisterExtension
    static final SessionExtension env = new SessionExtension().withAuthEnabled( true );

    protected BoltStateMachineV4 newStateMachine()
    {
        return (BoltStateMachineV4) env.newMachine( BoltProtocolV4.VERSION, BOLT_CHANNEL );
    }

    @Test
    void shouldGiveCredentialsExpiredStatusOnExpiredCredentials() throws Throwable
    {
        // Given it is important for client applications to programmatically
        // identify expired credentials as the cause of not being authenticated
        BoltStateMachine machine = newStateMachine();
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // When
        var hello = BoltV4Messages.hello( newBasicAuthToken( "neo4j", "neo4j" ) );

        machine.process( hello, recorder );
        machine.process( BoltV4Messages.run( "CREATE ()" ), recorder );

        // Then
        assertThat( recorder.nextResponse(), succeededWithMetadata( "credentials_expired", TRUE ) );
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Security.CredentialsExpired ) );
    }

    @Test
    void shouldGiveKernelVersionOnInit() throws Throwable
    {
        // Given it is important for client applications to programmatically
        // identify expired credentials as the cause of not being authenticated
        BoltStateMachine machine = newStateMachine();
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        String version = "Neo4j/" + Version.getNeo4jVersion();

        // When
        var hello = BoltV4Messages.hello( newBasicAuthToken( "neo4j", "neo4j" ) );

        machine.process( hello, recorder );
        machine.process( BoltV4Messages.run( "CREATE ()" ), recorder );

        // Then
        assertThat( recorder.nextResponse(), succeededWithMetadata( "server", stringValue( version ) ) );
    }

    @Test
    void shouldCloseConnectionAfterAuthenticationFailure() throws Throwable
    {
        // Given
        BoltStateMachine machine = newStateMachine();
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // When... then
        var hello = BoltV4Messages.hello( newBasicAuthToken( "neo4j", "j4oen" ) );
        verifyKillsConnection( () -> machine.process( hello, recorder ) );

        // ...and
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Security.Unauthorized ) );
    }
}
