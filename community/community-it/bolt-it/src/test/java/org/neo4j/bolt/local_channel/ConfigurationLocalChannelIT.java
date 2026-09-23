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
package org.neo4j.bolt.local_channel;

import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Connected;
import org.neo4j.bolt.test.annotation.connection.transport.IncludeTransport;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.test.connection.setup.SettingBuilder;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

public class ConfigurationLocalChannelIT {

    @EphemeralTestDirectoryExtension
    @Neo4jWithSocketExtension
    @BoltTestExtension
    @IncludeTransport({TransportType.LOCAL})
    static class ProtocolVersionIT extends AbstractLocalChannelIT {

        @SettingsFunction
        protected static void customizeSettings(SettingBuilder settings) {
            settings.set(
                    BoltConnectorInternalSettings.max_protocol_version,
                    BoltConnectorInternalSettings.ConfiguredProtocolVersion.fromString("6.0"));
            settings.set(BoltConnector.enable_object_messages_local_connector, true);
        }

        @TransportTest
        void shouldAuthenticate(@Connected BoltTestConnection connection) throws Exception {
            connection.unwired(AbstractLocalChannelIT::authenticate);
        }
    }
}
