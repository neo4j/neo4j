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
package org.neo4j.bolt.v3.runtime.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.testing.RecordedBoltResponse;
import org.neo4j.bolt.v1.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.request.InitMessage;
import org.neo4j.bolt.v1.messaging.request.InterruptSignal;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v3.BoltStateMachineV3;
import org.neo4j.bolt.v3.messaging.request.BeginMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.bolt.v3.runtime.ReadyState;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.Version;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.bolt.testing.BoltMatchers.failedWithStatus;
import static org.neo4j.bolt.testing.BoltMatchers.succeededWithMetadata;
import static org.neo4j.bolt.testing.BoltMatchers.verifyKillsConnection;
import static org.neo4j.bolt.v1.runtime.BoltStateMachineV1SPI.BOLT_SERVER_VERSION_PREFIX;
import static org.neo4j.bolt.v3.messaging.request.CommitMessage.COMMIT_MESSAGE;
import static org.neo4j.bolt.v3.messaging.request.GoodbyeMessage.GOODBYE_MESSAGE;
import static org.neo4j.bolt.v3.messaging.request.RollbackMessage.ROLLBACK_MESSAGE;

class ConnectedStateIT extends BoltStateMachineStateTestBase
{
    @Test
    void shouldHandleHelloMessage() throws Throwable
    {
        // Given
        BoltStateMachineV3 machine = newStateMachine();
        BoltResponseRecorder recorder = new BoltResponseRecorder();

        // When
        machine.process( newHelloMessage(), recorder );

        // Then
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeededWithMetadata( "server", BOLT_SERVER_VERSION_PREFIX + Version.getNeo4jVersion() ) );
        assertThat( response, succeededWithMetadata( "connection_id", "conn-v3-test-boltchannel-id" ) );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );
    }

    @ParameterizedTest
    @MethodSource( "illegalV3Messages" )
    void shouldCloseConnectionOnIllegalV3Messages( RequestMessage message ) throws Throwable
    {
        shouldCloseConnectionOnIllegalMessages( message );
    }

    @ParameterizedTest
    @MethodSource( "illegalV2Messages" )
    void shouldCloseConnectionOnIllegalV2Messages( RequestMessage message ) throws Throwable
    {
        shouldCloseConnectionOnIllegalMessages( message );
    }

    private void shouldCloseConnectionOnIllegalMessages( RequestMessage message ) throws InterruptedException
    {
        // Given
        BoltStateMachineV3 machine = newStateMachine();

        // when
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        verifyKillsConnection( () -> machine.process( message, recorder ) );

        // then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Request.Invalid ) );
        assertNull( machine.state() );
    }

    private static Stream<RequestMessage> illegalV3Messages() throws BoltIOException
    {
        return Stream.of( new RunMessage( "RETURN 1", EMPTY_PARAMS, EMPTY_PARAMS ), DiscardAllMessage.INSTANCE, PullAllMessage.INSTANCE, new BeginMessage(),
                COMMIT_MESSAGE, ROLLBACK_MESSAGE, InterruptSignal.INSTANCE, ResetMessage.INSTANCE, GOODBYE_MESSAGE );
    }

    private static Stream<RequestMessage> illegalV2Messages()
    {
        return Stream.of( new org.neo4j.bolt.v1.messaging.request.RunMessage( "RETURN 1", EMPTY_PARAMS ), new InitMessage( USER_AGENT, emptyMap() ) );
    }
}
