/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.authentication;

import static java.time.Duration.ofSeconds;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.setup.FactoryFunction;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.test.connection.setup.SettingBuilder;
import org.neo4j.bolt.test.util.ServerUtil;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.assertions.DiagnosticRecordAssertions;
import org.neo4j.bolt.testing.assertions.FailureMetadataAssertions;
import org.neo4j.bolt.testing.assertions.GqlMessageParameters;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.messages.BoltV51Wire;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@ExtendWith(OtherThreadExtension.class)
class PreAuthLimitIT {

    private static final FailureMetadataAssertions EXCEEDED_LIMIT_ASSERTIONS = FailureMetadataAssertions.create()
            .hasLegacyStatus(Status.Request.Invalid)
            .hasLegacyMessage("Value of size 1023 exceeded limit of 1000")
            .hasStatus(
                    GqlStatusInfoCodes.STATUS_22N56,
                    GqlMessageParameters.create().withInt(1000))
            .hasDescription(
                    "error: data exception - protocol message length limit overflow. Protocol message length limit exceeded (limit: 1000).")
            .hasDiagnosticRecord(
                    DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR));

    private final AssertableLogProvider internalLogProvider = new AssertableLogProvider();

    @Inject
    private Neo4jWithSocket server;

    @FactoryFunction
    void customizeDatabase(TestDatabaseManagementServiceBuilder factory) {
        factory.setInternalLogProvider(this.internalLogProvider);
    }

    @SettingsFunction
    static void customizeSettings(SettingBuilder settings) {
        settings.set(BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_max_inbound_bytes, 1000L)
                .set(BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_timeout, ofSeconds(2));
    }

    @BeforeEach
    void prepare() throws ProcedureException {
        ServerUtil.installSleepProcedure(this.server);
    }

    @AfterEach
    void cleanup() {
        this.internalLogProvider.clear();
    }

    @ProtocolTest
    void shouldFailDueToMessageBeingTooLargeInUnauthenticatedState(@VersionSelected BoltTestConnection connection) {
        connection.send(createValidBufferOf1023bytes(BoltV51Wire.MESSAGE_TAG_HELLO));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(EXCEEDED_LIMIT_ASSERTIONS)
                .isEventuallyTerminated();
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 1))
    void shouldFailDueToMessageBeingTooLargeInAuthenticationState(
            BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        connection.send(createValidBufferOf1023bytes(BoltV51Wire.MESSAGE_TAG_HELLO));
        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(EXCEEDED_LIMIT_ASSERTIONS)
                .isEventuallyTerminated();
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 6, minor = 0))
    void shouldFailDueToMessageBeingTooLargeInAuthenticationStateAfterLoggingOut(
            BoltWire wire, @Authenticated BoltTestConnection connection) {
        connection.send(wire.logoff());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        connection.send(createValidBufferOf1023bytes(BoltV51Wire.MESSAGE_TAG_HELLO));
        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(EXCEEDED_LIMIT_ASSERTIONS)
                .isEventuallyTerminated();
        ;
    }

    @ProtocolTest
    void whenAuthenticatedShouldBeNoLimitOnMessageSize(BoltWire wire, @Authenticated BoltTestConnection connection) {
        connection.send(createValidBufferOf1023bytes(BoltV51Wire.MESSAGE_TAG_BEGIN));

        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }

    private PackstreamBuf createValidBufferOf1023bytes(short messageTag) {
        PackstreamBuf buff = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, messageTag))
                .writeMapHeader(1)
                .writeString("user_agent")
                .writeString("test/1")
                .writeString("2");

        for (int i = 0; i < 500; i++) {
            buff.writeInt8((byte) 0x1);
        }

        return buff;
    }

    @ProtocolTest
    void whenInUnauthenticatedStateShouldErrorIfConnectionOpenTooLong(
            BoltWire wire, @VersionSelected BoltTestConnection connection) throws InterruptedException {
        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();

        Thread.sleep(1000); // This is to ensure the log contains the message before it runs.

        LogAssertions.assertThat(internalLogProvider)
                .forLevel(AssertableLogProvider.Level.ERROR)
                .containsMessagesEventually(
                        ofSeconds(4).toMillis(), "as the client failed to authenticate within 2000 ms.");
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 1))
    void whenInAuthenticationStateShouldErrorIfConnectionOpenTooLong(
            BoltWire wire, @VersionSelected BoltTestConnection connection) throws InterruptedException {
        connection.send(wire.hello());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();

        LogAssertions.assertThat(internalLogProvider)
                .forLevel(AssertableLogProvider.Level.ERROR)
                .containsMessagesEventually(
                        ofSeconds(4).toMillis(),
                        "as the server failed to handle an authentication request within 2000 ms.");
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 1))
    void whenInAuthenticationStateAfterLogoffShouldErrorIfConnectionOpenTooLong(
            BoltWire wire, @Authenticated BoltTestConnection connection) throws InterruptedException {
        connection.send(wire.logoff());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();

        LogAssertions.assertThat(internalLogProvider)
                .forLevel(AssertableLogProvider.Level.ERROR)
                .containsMessagesEventually(
                        ofSeconds(4).toMillis(), "as the client failed to authenticate within 2000 ms.");
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 1))
    void whenLoggedInShouldBeNoAuthenticationTimeout(BoltWire wire, @VersionSelected BoltTestConnection connection)
            throws InterruptedException {
        connection.send(wire.hello());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        connection.send(wire.logon());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        Thread.sleep(ofSeconds(5)); // this should be fine in a test.

        connection.send(wire.begin());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }
}
