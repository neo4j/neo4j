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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Connected;
import org.neo4j.bolt.test.annotation.connection.transport.IncludeTransport;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.boltmessages.AccessMode;
import org.neo4j.boltmessages.notifications.DisabledNotificationsConfig;
import org.neo4j.boltmessages.request.streaming.DiscardMessage;
import org.neo4j.boltmessages.request.transaction.BeginMessage;
import org.neo4j.boltmessages.request.transaction.CommitMessage;
import org.neo4j.boltmessages.request.transaction.RunMessage;
import org.neo4j.notifications.StandardGqlStatusObject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValueBuilder;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@IncludeTransport({TransportType.LOCAL})
public class DiscardLocalChannelIT extends AbstractLocalChannelIT {
    private final AnyValue param = Values.intValue(123);

    @TransportTest
    void shouldDiscardRecordsOnSimpleQuery(@Connected BoltTestConnection connection) throws Exception {
        connection.unwired(unwired -> {
            authenticate(unwired);

            var parametersBuilder = new MapValueBuilder();
            parametersBuilder.add("param", param);
            unwired.sendRequest(new RunMessage(
                            "RETURN $param as value",
                            parametersBuilder.build(),
                            List.of(),
                            Duration.of(10, ChronoUnit.SECONDS),
                            AccessMode.READ,
                            Map.of(),
                            "neo4j",
                            null,
                            DisabledNotificationsConfig.getInstance()))
                    .sendRequest(new DiscardMessage(1000, -1));

            assertSuccess(unwired.receiveResponse(), assertSuccessHasTFirst(), assertSuccessHasFields("value"));

            assertSuccess(
                    unwired.receiveResponse(),
                    assertSuccessDb("neo4j"),
                    assertSuccessType("r"),
                    assertSuccessHasTLast(),
                    assertSuccessHasBookmark(),
                    assertSuccessHasStatuses(status(
                            StandardGqlStatusObject.SUCCESS.gqlStatus(),
                            StandardGqlStatusObject.SUCCESS.statusDescription())));
        });
    }

    @TransportTest
    void shouldDiscardRecordOnSingleQueryInTx(@Connected BoltTestConnection connection) throws Exception {
        connection.unwired(unwired -> {
            authenticate(unwired);

            var parametersBuilder = new MapValueBuilder();
            parametersBuilder.add("param", param);

            unwired.sendRequest(new BeginMessage(
                            List.of(), Duration.of(10, ChronoUnit.SECONDS), AccessMode.READ, Map.of(), "neo4j"))
                    .sendRequest(new RunMessage("RETURN $param as value", parametersBuilder.build()))
                    .sendRequest(new DiscardMessage(1000, -1))
                    .sendRequest(CommitMessage.getInstance());

            assertSuccess(unwired.receiveResponse(), assertSuccessEmpty());
            assertSuccess(unwired.receiveResponse(), assertSuccessHasTFirst(), assertSuccessHasFields("value"));

            assertSuccess(
                    unwired.receiveResponse(),
                    assertSuccessHasTLast(),
                    assertSuccessDb("neo4j"),
                    assertSuccessType("r"),
                    assertSuccessHasStatuses(status(
                            StandardGqlStatusObject.SUCCESS.gqlStatus(),
                            StandardGqlStatusObject.SUCCESS.statusDescription())));
            assertSuccess(unwired.receiveResponse(), assertSuccessHasBookmark());
        });
    }
}
