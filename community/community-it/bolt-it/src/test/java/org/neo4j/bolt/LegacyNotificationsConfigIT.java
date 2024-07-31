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

import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.testing.messages.factory.NotificationsMessageBuilder;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.graphdb.NotificationCategory;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.kernel.impl.query.NotificationConfiguration;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@ExtendWith(OtherThreadExtension.class)
public class LegacyNotificationsConfigIT {

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 4, range = 2)})
    public void shouldReturnWarning(BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {
        connection.send(wire.hello(x -> x.withSeverity(NotificationConfiguration.Severity.WARNING)));
        connection.send(wire.logon());
        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
        // Then
        assertThat(connection)
                .receivesSuccessWithNotification(
                        "Neo.ClientNotification.Statement.UnknownLabelWarning",
                        "The provided label is not in the database.",
                        "One of the labels in your query is not available in the database, "
                                + "make sure you didn't misspell it or that the label is available when "
                                + "you run this statement in your application (the missing label name is: "
                                + "THIS_IS_NOT_A_LABEL)",
                        SeverityLevel.WARNING,
                        NotificationCategory.UNRECOGNIZED,
                        17,
                        1,
                        18);
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 4, range = 2)})
    public void shouldReturnSingleCartesianProductWarning(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {
        connection.send(wire.hello(x -> {
            x.withDisabledCategories(Set.of(NotificationConfiguration.Category.UNRECOGNIZED));
            return x;
        }));
        connection.send(wire.logon());
        connection
                .send(wire.run("MATCH (n:thisLabelDoesNotExist), (m:thisLabelDoesNotExist) return m, n"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
        // Then
        assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(((ArrayList<?>) meta.get("notifications")).size())
                        .isEqualTo(1));
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 4, range = 2)})
    public void shouldReturnSingleUnboundedVariableLengthWarning(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {
        connection.send(wire.hello(x -> {
            x.withDisabledCategories(Set.of(NotificationConfiguration.Category.UNRECOGNIZED));
            return x;
        }));
        connection.send(wire.logon());
        connection
                .send(wire.run("MATCH shortestPath((n:A)-[*]->(m:B)) return m, n"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
        // Then
        assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(((ArrayList<?>) meta.get("notifications")).size())
                        .isEqualTo(1));
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 4, range = 2)})
    public void shouldReturnSingleRepeatedRelationshipWarning(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {
        connection.send(wire.hello());
        connection.send(wire.logon());
        connection.send(wire.run("MATCH ()-[r]-()-[r]-() RETURN r AS r")).send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
        // Then
        assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(((ArrayList<?>) meta.get("notifications")).size())
                        .isEqualTo(1));
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 4, range = 2)})
    public void shouldSendFailureWithUnknownSeverity(BoltWire wire, @VersionSelected TransportConnection connection)
            throws Throwable {
        connection.send(wire.hello(x -> x.withUnknownSeverity("WANING")));

        assertThat(connection).receivesFailure();
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 4, range = 2)})
    public void shouldSendFailureWithUnknownCategory(BoltWire wire, @VersionSelected TransportConnection connection)
            throws Throwable {
        connection.send(wire.hello(x -> x.withUnknownDisabledCategories(List.of("Pete"))));

        assertThat(connection).receivesFailure();
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 4, range = 2)})
    public void shouldNotReturnNotificationsWhenAllDisabled(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {

        connection.send(wire.hello(NotificationsMessageBuilder::withDisabledNotifications));
        connection.send(wire.logon());
        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
        assertThat(connection).receivesSuccess(x -> Assertions.assertThat(x).doesNotContainKey("notifications"));
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 4, range = 2)})
    public void shouldReturnMultipleNotifications(BoltWire wire, @VersionSelected TransportConnection connection)
            throws Throwable {

        connection.send(wire.hello());
        connection.send(wire.logon());
        connection
                .send(wire.run("MATCH (a:Person) CALL { MATCH (a:Label) RETURN a AS aLabel } RETURN a"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);

        assertThat(connection).receivesSuccess(x -> {
            Assertions.assertThat(x).containsKey("notifications");
            Assertions.assertThat((ArrayList<?>) x.get("notifications")).hasSize(3);
        });
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 4, range = 2)})
    public void shouldEnableNotificationsForQuery(BoltWire wire, @VersionSelected TransportConnection connection)
            throws Throwable {
        connection.send(wire.hello(NotificationsMessageBuilder::withDisabledNotifications));
        connection.send(wire.logon());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess(2);

        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());
        assertThat(connection).receivesSuccess().receivesSuccess(x -> Assertions.assertThat(x)
                .doesNotContainKey("notifications"));
        connection
                .send(wire.run(
                        "EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)",
                        x -> x.withSeverity(NotificationConfiguration.Severity.INFORMATION)))
                .send(wire.pull());

        assertThat(connection)
                .receivesSuccess()
                .receivesSuccessWithNotification(
                        "Neo.ClientNotification.Statement.UnknownLabelWarning",
                        "The provided label is not in the database.",
                        "One of the labels in your query is not available in the database, "
                                + "make sure you didn't misspell it or that the label is available when "
                                + "you run this statement in your application (the missing label name is: "
                                + "THIS_IS_NOT_A_LABEL)",
                        SeverityLevel.WARNING,
                        NotificationCategory.UNRECOGNIZED,
                        17,
                        1,
                        18);
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 4, range = 2)})
    public void shouldSendFailureOnRunWithUnknownSeverity(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {
        connection.send(wire.hello(NotificationsMessageBuilder::withDisabledNotifications));
        connection.send(wire.logon());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess(2);

        connection.send(wire.run("RETURN 1 as n", x -> x.withUnknownSeverity("boom")));

        assertThat(connection).receivesFailure();
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 4, range = 2)})
    public void shouldSendFailureOnRunWithUnknownCategory(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {

        connection.send(wire.hello(NotificationsMessageBuilder::withDisabledNotifications));
        connection.send(wire.logon());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess(2);

        connection.send(wire.run("RETURN 1 as n", x -> x.withUnknownDisabledCategories(List.of("boom"))));

        assertThat(connection).receivesFailure();
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 4, range = 2)})
    public void shouldEnableNotificationsForQueryUsingCategories(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {

        connection.send(wire.hello(NotificationsMessageBuilder::withDisabledNotifications));
        connection.send(wire.logon());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess(2);

        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());
        assertThat(connection).receivesSuccess().receivesSuccess(x -> Assertions.assertThat(x)
                .doesNotContainKey("notifications"));

        connection
                .send(wire.run(
                        "EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)",
                        x -> x.withDisabledCategories(Collections.emptyList())))
                .send(wire.pull());

        assertThat(connection)
                .receivesSuccess()
                .receivesSuccessWithNotification(
                        "Neo.ClientNotification.Statement.UnknownLabelWarning",
                        "The provided label is not in the database.",
                        "One of the labels in your query is not available in the database, "
                                + "make sure you didn't misspell it or that the label is available when "
                                + "you run this statement in your application (the missing label name is: "
                                + "THIS_IS_NOT_A_LABEL)",
                        SeverityLevel.WARNING,
                        NotificationCategory.UNRECOGNIZED,
                        17,
                        1,
                        18);
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 4, range = 2)})
    public void shouldEnableNotificationsInBegin(BoltWire wire, @VersionSelected TransportConnection connection)
            throws Throwable {

        connection.send(wire.hello(NotificationsMessageBuilder::withDisabledNotifications));
        connection.send(wire.logon());
        assertThat(connection).receivesSuccess(2);

        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());
        assertThat(connection).receivesSuccess().receivesSuccess(x -> Assertions.assertThat(x)
                .doesNotContainKey("notifications"));

        connection.send(wire.begin(x -> x.withSeverity(NotificationConfiguration.Severity.WARNING)));
        assertThat(connection).receivesSuccess();

        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());

        assertThat(connection)
                .receivesSuccess()
                .receivesSuccessWithNotification(
                        "Neo.ClientNotification.Statement.UnknownLabelWarning",
                        "The provided label is not in the database.",
                        "One of the labels in your query is not available in the database, "
                                + "make sure you didn't misspell it or that the label is available when "
                                + "you run this statement in your application (the missing label name is: "
                                + "THIS_IS_NOT_A_LABEL)",
                        SeverityLevel.WARNING,
                        NotificationCategory.UNRECOGNIZED,
                        17,
                        1,
                        18);
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 4, range = 2)})
    public void shouldEnableNotificationsInBeginWithCategories(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {

        connection.send(wire.hello(NotificationsMessageBuilder::withDisabledNotifications));
        connection.send(wire.logon());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess(2);

        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());
        assertThat(connection).receivesSuccess().receivesSuccess(x -> Assertions.assertThat(x)
                .doesNotContainKey("notifications"));

        connection.send(wire.begin(x -> x.withDisabledCategories(Collections.emptyList())));
        assertThat(connection).receivesSuccess();

        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());

        assertThat(connection)
                .receivesSuccess()
                .receivesSuccessWithNotification(
                        "Neo.ClientNotification.Statement.UnknownLabelWarning",
                        "The provided label is not in the database.",
                        "One of the labels in your query is not available in the database, "
                                + "make sure you didn't misspell it or that the label is available when "
                                + "you run this statement in your application (the missing label name is: "
                                + "THIS_IS_NOT_A_LABEL)",
                        SeverityLevel.WARNING,
                        NotificationCategory.UNRECOGNIZED,
                        17,
                        1,
                        18);
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 4, range = 2)})
    public void shouldNotReturnNotificationsInDisabledCategories(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {
        connection.send(wire.hello(x -> x.withDisabledCategories(
                List.of(NotificationConfiguration.Category.GENERIC, NotificationConfiguration.Category.UNRECOGNIZED))));
        connection.send(wire.logon());
        connection
                .send(wire.run("MATCH (a:Person) CALL { MATCH (a:Label) RETURN a AS aLabel } RETURN a"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);

        assertThat(connection).receivesSuccess(x -> Assertions.assertThat(x).doesNotContainKey("notifications"));
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 4, range = 2)})
    public void shouldNotReturnNotificationsWhenNotHighEnoughSeverity(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {
        connection.send(wire.hello(x -> x.withSeverity(NotificationConfiguration.Severity.WARNING)));
        connection.send(wire.logon());
        connection
                .send(wire.run("MATCH (a:Person) CALL { MATCH (a:Label) RETURN a AS aLabel } RETURN a"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);

        assertThat(connection)
                .receivesSuccess(x ->
                        Assertions.assertThat(x.get("notifications").toString()).doesNotContain("GENERIC"));
    }
}
