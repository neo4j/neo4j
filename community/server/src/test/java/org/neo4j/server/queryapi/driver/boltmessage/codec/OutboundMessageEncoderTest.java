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
package org.neo4j.server.queryapi.driver.boltmessage.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.NotificationClassification;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.NotificationSeverity;
import org.neo4j.bolt.connection.netty.impl.messaging.request.BeginMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.CommitMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.DiscardMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.GoodbyeMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.HelloMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.LogoffMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.LogonMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.PullMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.RequestMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.ResetMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.RollbackMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.RunWithMetadataMessage;
import org.neo4j.boltmessages.notifications.DefaultNotificationsConfig;
import org.neo4j.boltmessages.notifications.DisabledNotificationsConfig;
import org.neo4j.boltmessages.notifications.SelectiveNotificationsConfig;
import org.neo4j.boltmessages.request.connection.RoutingContext;
import org.neo4j.boltmessages.request.transaction.RunMessage;
import org.neo4j.driver.internal.value.BoltValueFactory;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.kernel.impl.query.NotificationConfiguration;
import org.neo4j.server.queryapi.driver.boltmessage.pipeline.OutboundMessageEncoder;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

class OutboundMessageEncoderTest {

    static Stream<Arguments> values() {
        return Stream.of(
                Arguments.of(
                        new HelloMessage(
                                "user_agent",
                                new BoltAgent("query", "high", "java", "i wont tell you"),
                                Map.of(
                                        "principal", new StringValue("bla"),
                                        "credentials", new StringValue("bla bla")),
                                Map.of("proxy", "some proxy"),
                                true,
                                NotificationConfig.defaultConfig(),
                                false,
                                BoltValueFactory.getInstance()),
                        new org.neo4j.boltmessages.request.authentication.HelloMessage(
                                "user_agent",
                                List.of("utc"),
                                new RoutingContext(true, Map.of("proxy", "some proxy")),
                                Map.of(
                                        "principal", "bla",
                                        "credentials", "bla bla"),
                                DefaultNotificationsConfig.getInstance(),
                                Map.of(
                                        "product", "query",
                                        "platform", "high",
                                        "language", "java",
                                        "language_details", "i wont tell you"))),
                Arguments.of(
                        new LogonMessage(
                                Map.of(
                                        "scheme", new StringValue("basic"),
                                        "principal", new StringValue("bla"),
                                        "credentials", new StringValue("bla bla")),
                                BoltValueFactory.getInstance()),
                        new org.neo4j.boltmessages.request.authentication.LogonMessage(Map.of(
                                "scheme",
                                "basic",
                                "principal",
                                "bla",
                                "credentials",
                                "bla bla".getBytes(StandardCharsets.UTF_8)))),
                Arguments.of(
                        LogoffMessage.INSTANCE,
                        org.neo4j.boltmessages.request.authentication.LogoffMessage.getInstance()),
                Arguments.of(
                        RunWithMetadataMessage.unmanagedTxRunMessage("RETURN 1", Map.of()),
                        new RunMessage(
                                "RETURN 1",
                                MapValue.EMPTY,
                                List.of(),
                                null,
                                org.neo4j.boltmessages.AccessMode.WRITE,
                                Map.of(),
                                null,
                                null,
                                DefaultNotificationsConfig.getInstance())),
                Arguments.of(
                        RunWithMetadataMessage.autoCommitTxRunMessage(
                                "RETURN 1",
                                Map.of(),
                                null,
                                Map.of(),
                                DatabaseName.defaultDatabase(),
                                AccessMode.WRITE,
                                Set.of(),
                                null,
                                null,
                                false,
                                mock(LoggingProvider.class),
                                BoltValueFactory.getInstance()),
                        new RunMessage(
                                "RETURN 1",
                                MapValue.EMPTY,
                                List.of(),
                                null,
                                org.neo4j.boltmessages.AccessMode.WRITE,
                                Map.of(),
                                null,
                                null,
                                DefaultNotificationsConfig.getInstance())),
                Arguments.of(
                        RunWithMetadataMessage.autoCommitTxRunMessage(
                                "RETURN 1",
                                Map.of(),
                                null,
                                Map.of(),
                                DatabaseName.defaultDatabase(),
                                AccessMode.WRITE,
                                Set.of(),
                                null,
                                new NotificationConfig(NotificationSeverity.OFF, Set.of()),
                                false,
                                mock(LoggingProvider.class),
                                BoltValueFactory.getInstance()),
                        new RunMessage(
                                "RETURN 1",
                                MapValue.EMPTY,
                                List.of(),
                                null,
                                org.neo4j.boltmessages.AccessMode.WRITE,
                                Map.of(),
                                null,
                                null,
                                DisabledNotificationsConfig.getInstance())),
                Arguments.of(
                        RunWithMetadataMessage.autoCommitTxRunMessage(
                                "RETURN 1",
                                Map.of(),
                                null,
                                Map.of(),
                                DatabaseName.defaultDatabase(),
                                AccessMode.WRITE,
                                Set.of(),
                                null,
                                new NotificationConfig(
                                        NotificationSeverity.WARNING,
                                        Set.of(new NotificationClassification(
                                                NotificationClassification.Type.PERFORMANCE))),
                                false,
                                mock(LoggingProvider.class),
                                BoltValueFactory.getInstance()),
                        new RunMessage(
                                "RETURN 1",
                                MapValue.EMPTY,
                                List.of(),
                                null,
                                org.neo4j.boltmessages.AccessMode.WRITE,
                                Map.of(),
                                null,
                                null,
                                new SelectiveNotificationsConfig(
                                        NotificationConfiguration.Severity.WARNING,
                                        Set.of(NotificationConfiguration.Category.PERFORMANCE)))),
                Arguments.of(
                        new BeginMessage(
                                Set.of(),
                                Duration.ofSeconds(10),
                                Map.of("meta", new StringValue("data")),
                                AccessMode.READ,
                                DatabaseName.defaultDatabase(),
                                null,
                                "hi",
                                NotificationConfig.defaultConfig(),
                                false,
                                mock(LoggingProvider.class),
                                BoltValueFactory.getInstance()),
                        new org.neo4j.boltmessages.request.transaction.BeginMessage(
                                List.of(),
                                Duration.ofSeconds(10),
                                org.neo4j.boltmessages.AccessMode.READ,
                                Map.of("meta", Values.stringValue("data")),
                                null,
                                null,
                                null,
                                DefaultNotificationsConfig.getInstance())),
                Arguments.of(
                        new BeginMessage(
                                Set.of(),
                                Duration.ofSeconds(10),
                                Map.of("meta", new StringValue("data")),
                                AccessMode.READ,
                                DatabaseName.defaultDatabase(),
                                null,
                                "hi",
                                new NotificationConfig(NotificationSeverity.OFF, Set.of()),
                                false,
                                mock(LoggingProvider.class),
                                BoltValueFactory.getInstance()),
                        new org.neo4j.boltmessages.request.transaction.BeginMessage(
                                List.of(),
                                Duration.ofSeconds(10),
                                org.neo4j.boltmessages.AccessMode.READ,
                                Map.of("meta", Values.stringValue("data")),
                                null,
                                null,
                                null,
                                DisabledNotificationsConfig.getInstance())),
                Arguments.of(
                        new BeginMessage(
                                Set.of(),
                                Duration.ofSeconds(10),
                                Map.of("meta", new StringValue("data")),
                                AccessMode.READ,
                                DatabaseName.defaultDatabase(),
                                null,
                                "hi",
                                new NotificationConfig(
                                        NotificationSeverity.WARNING,
                                        Set.of(new NotificationClassification(
                                                NotificationClassification.Type.PERFORMANCE))),
                                false,
                                mock(LoggingProvider.class),
                                BoltValueFactory.getInstance()),
                        new org.neo4j.boltmessages.request.transaction.BeginMessage(
                                List.of(),
                                Duration.ofSeconds(10),
                                org.neo4j.boltmessages.AccessMode.READ,
                                Map.of("meta", Values.stringValue("data")),
                                null,
                                null,
                                null,
                                new SelectiveNotificationsConfig(
                                        NotificationConfiguration.Severity.WARNING,
                                        Set.of(NotificationConfiguration.Category.PERFORMANCE)))),
                Arguments.of(
                        new PullMessage(10, 1, BoltValueFactory.getInstance()),
                        new org.neo4j.boltmessages.request.streaming.PullMessage(10, 1)),
                Arguments.of(
                        new DiscardMessage(10, 1, BoltValueFactory.getInstance()),
                        new org.neo4j.boltmessages.request.streaming.DiscardMessage(10, 1)),
                Arguments.of(
                        CommitMessage.COMMIT, org.neo4j.boltmessages.request.transaction.CommitMessage.getInstance()),
                Arguments.of(
                        RollbackMessage.ROLLBACK,
                        org.neo4j.boltmessages.request.transaction.RollbackMessage.getInstance()),
                Arguments.of(
                        GoodbyeMessage.GOODBYE, org.neo4j.boltmessages.request.connection.GoodbyeMessage.getInstance()),
                Arguments.of(ResetMessage.RESET, org.neo4j.boltmessages.request.connection.ResetMessage.getInstance()));
    }

    @ParameterizedTest
    @MethodSource("values")
    void encode(RequestMessage requestMessage, org.neo4j.boltmessages.request.RequestMessage expectedBoltMessage) {
        assertThat(OutboundMessageEncoder.encode(requestMessage))
                .usingRecursiveComparison()
                .isEqualTo(expectedBoltMessage);
    }
}
