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
package org.neo4j.bolt;

import static org.neo4j.bolt.testing.messages.AbstractBoltWire.MESSAGE_TAG_HELLO;
import static org.neo4j.bolt.testing.messages.AbstractBoltWire.MESSAGE_TAG_LOGON;

import org.junit.jupiter.api.AfterEach;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Negotiated;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.setup.FactoryFunction;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.setup.preset.EnableAuthentication;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.test.connection.setup.SettingBuilder;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.Level;
import org.neo4j.logging.LogAssertions;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@EnableAuthentication
public class AuthenticationProtocolLimitIT {

    private final AssertableLogProvider internalLogProvider = new AssertableLogProvider();
    private final AssertableLogProvider userLogProvider = new AssertableLogProvider();

    @FactoryFunction
    void customizeDatabase(TestDatabaseManagementServiceBuilder factory) {
        factory.setInternalLogProvider(this.internalLogProvider);
        factory.setUserLogProvider(this.userLogProvider);
    }

    @SettingsFunction
    static void customizeSettings(SettingBuilder settings) {
        settings.set(BoltConnectorInternalSettings.bolt_unauth_connection_max_structure_depth, 4);
        settings.set(BoltConnectorInternalSettings.bolt_unauth_connection_max_structure_elements, 64);
    }

    @AfterEach
    void cleanup() {
        this.internalLogProvider.clear();
        this.userLogProvider.clear();
    }

    @ProtocolTest
    @IncludeWire(until = @Version(major = 5, minor = 0))
    public void shouldRejectVeryLongListsDuringHello(@VersionSelected BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_HELLO))
                .writeListHeader(128)
                .raw());

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();

        LogAssertions.assertThat(this.internalLogProvider)
                .forLevel(Level.ERROR)
                .containsMessagesOnce("Message has exceeded maximum permitted complexity of 64 elements");
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 1))
    public void shouldRejectVeryLongListsDuringLogin(@Negotiated BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_LOGON))
                .writeListHeader(128)
                .raw());

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();

        LogAssertions.assertThat(this.internalLogProvider)
                .forLevel(Level.ERROR)
                .containsMessagesOnce("Message has exceeded maximum permitted complexity of 64 elements");
    }

    @ProtocolTest
    @IncludeWire(until = @Version(major = 5, minor = 0))
    public void shouldRejectVeryLongStructsDuringHello(@VersionSelected BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_HELLO))
                .writeStructHeader(new StructHeader(128, (short) 0x42))
                .raw());

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();

        LogAssertions.assertThat(this.internalLogProvider)
                .forLevel(Level.ERROR)
                .containsMessagesOnce("Message has exceeded maximum permitted complexity of 64 elements");
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 1))
    public void shouldRejectVeryLongStructsDuringLogin(@Negotiated BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_LOGON))
                .writeStructHeader(new StructHeader(128, (short) 0x42))
                .raw());

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();

        LogAssertions.assertThat(this.internalLogProvider)
                .forLevel(Level.ERROR)
                .containsMessagesOnce("Message has exceeded maximum permitted complexity of 64 elements");
    }

    @ProtocolTest
    @IncludeWire(until = @Version(major = 5, minor = 0))
    public void shouldRejectVeryLongRootStructsDuringHello(@VersionSelected BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(128, MESSAGE_TAG_HELLO))
                .raw());

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();

        LogAssertions.assertThat(this.internalLogProvider)
                .forLevel(Level.ERROR)
                .containsMessagesOnce("Message has exceeded maximum permitted complexity of 64 elements");
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 1))
    public void shouldRejectVeryLongRootStructsDuringLogin(@Negotiated BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(128, MESSAGE_TAG_LOGON))
                .raw());

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();

        LogAssertions.assertThat(this.internalLogProvider)
                .forLevel(Level.ERROR)
                .containsMessagesOnce("Message has exceeded maximum permitted complexity of 64 elements");
    }

    @ProtocolTest
    @IncludeWire(until = @Version(major = 5, minor = 0))
    public void shouldRejectOverlyNestedInterleavedTypesDuringHello(@VersionSelected BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_HELLO))
                .writeMapHeader(1)
                .writeString("a")
                .writeListHeader(1)
                .writeMapHeader(1)
                .writeString("b")
                .writeListHeader(1)
                .writeBoolean(true)
                .raw());

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();

        LogAssertions.assertThat(this.internalLogProvider)
                .forLevel(Level.ERROR)
                .containsMessagesOnce("Message has exceeded maximum permitted complexity of 4 levels");
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 1))
    public void shouldRejectOverlyNestedInterleavedTypesDuringLogin(@Negotiated BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_LOGON))
                .writeMapHeader(1)
                .writeString("a")
                .writeListHeader(1)
                .writeMapHeader(1)
                .writeString("b")
                .writeListHeader(1)
                .writeBoolean(true)
                .raw());

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();

        LogAssertions.assertThat(this.internalLogProvider)
                .forLevel(Level.ERROR)
                .containsMessagesOnce("Message has exceeded maximum permitted complexity of 4 levels");
    }

    @ProtocolTest
    @IncludeWire(until = @Version(major = 5, minor = 0))
    public void shouldRejectOverlyNestedListsDuringHello(@VersionSelected BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_HELLO))
                .writeListHeader(1)
                .writeListHeader(1)
                .writeListHeader(1)
                .writeListHeader(1)
                .writeListHeader(1)
                .raw());

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();

        LogAssertions.assertThat(this.internalLogProvider)
                .forLevel(Level.ERROR)
                .containsMessagesOnce("Message has exceeded maximum permitted complexity of 4 levels");
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 1))
    public void shouldRejectOverlyNestedListsDuringLogon(@Negotiated BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_LOGON))
                .writeListHeader(1)
                .writeListHeader(1)
                .writeListHeader(1)
                .writeListHeader(1)
                .writeListHeader(1)
                .raw());

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();

        LogAssertions.assertThat(this.internalLogProvider)
                .forLevel(Level.ERROR)
                .containsMessagesOnce("Message has exceeded maximum permitted complexity of 4 levels");
    }

    @ProtocolTest
    @IncludeWire(until = @Version(major = 5, minor = 0))
    public void shouldRejectOverlyNestedStructsDuringHello(@VersionSelected BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_HELLO))
                .writeStructHeader(new StructHeader(1, (short) 0x42))
                .writeStructHeader(new StructHeader(1, (short) 0x43))
                .writeStructHeader(new StructHeader(1, (short) 0x44))
                .writeStructHeader(new StructHeader(1, (short) 0x45))
                .writeStructHeader(new StructHeader(1, (short) 0x45))
                .raw());

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();

        LogAssertions.assertThat(this.internalLogProvider)
                .forLevel(Level.ERROR)
                .containsMessagesOnce("Message has exceeded maximum permitted complexity of 4 levels");
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 1))
    public void shouldRejectOverlyNestedStructsDuringLogin(@Negotiated BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_LOGON))
                .writeStructHeader(new StructHeader(1, (short) 0x42))
                .writeStructHeader(new StructHeader(1, (short) 0x43))
                .writeStructHeader(new StructHeader(1, (short) 0x44))
                .writeStructHeader(new StructHeader(1, (short) 0x45))
                .writeStructHeader(new StructHeader(1, (short) 0x45))
                .raw());

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();

        LogAssertions.assertThat(this.internalLogProvider)
                .forLevel(Level.ERROR)
                .containsMessagesOnce("Message has exceeded maximum permitted complexity of 4 levels");
    }
}
