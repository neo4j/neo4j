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
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.diagnosticRecordPosition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.ExcludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.testing.messages.factory.NotificationsMessageBuilder;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.graphdb.NotificationCategory;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.kernel.impl.query.NotificationConfiguration;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.storable.Values;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@ExtendWith(OtherThreadExtension.class)
public class NotificationsConfigIT {

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldReturnSuccess(BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {
        connection.send(wire.hello(x -> x.withSeverity(NotificationConfiguration.Severity.NONE)));
        connection.send(wire.logon());
        connection.send(wire.run("RETURN 1")).send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
        BoltConnectionAssertions.assertThat(connection).receivesRecord(Values.intValue(1));

        // Then
        assertThat(connection).receivesSuccessWithStatus(GqlStatusInfoCodes.STATUS_00000);
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldReturnNoData(BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {
        connection.send(wire.hello(x -> x.withSeverity(NotificationConfiguration.Severity.NONE)));
        connection.send(wire.logon());
        connection.send(wire.run("MATCH (a) RETURN a")).send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
        // Then
        assertThat(connection).receivesSuccessWithStatus(GqlStatusInfoCodes.STATUS_02000);
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldReturnSuccessOmittedResult(BoltWire wire, @VersionSelected TransportConnection connection)
            throws Throwable {
        connection.send(wire.hello(x -> x.withSeverity(NotificationConfiguration.Severity.NONE)));
        connection.send(wire.logon());
        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
        // Then
        assertThat(connection).receivesSuccessWithStatus(GqlStatusInfoCodes.STATUS_00001);
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldReturnWarning(BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {
        connection.send(wire.hello(x -> x.withSeverity(NotificationConfiguration.Severity.WARNING)));
        connection.send(wire.logon());
        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
        // Then
        assertThat(connection)
                .receivesSuccessWithStatus(
                        GqlStatusInfoCodes.STATUS_01N50,
                        "The label `THIS_IS_NOT_A_LABEL` does not exist. Verify that the spelling is correct.",
                        "One of the labels in your query is not available in the database, "
                                + "make sure you didn't misspell it or that the label is available when "
                                + "you run this statement in your application (the missing label name is: "
                                + "THIS_IS_NOT_A_LABEL)",
                        "The provided label is not in the database.",
                        "Neo.ClientNotification.Statement.UnknownLabelWarning",
                        BoltConnectionAssertions.assertDiagnosticRecord(
                                SeverityLevel.WARNING,
                                NotificationCategory.UNRECOGNIZED,
                                Map.of("label", "THIS_IS_NOT_A_LABEL"),
                                diagnosticRecordPosition(18L, 1L, 17L)));
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldReturnMultipleCartesianProductWarning(
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
                .receivesSuccess(meta -> Assertions.assertThat(((ArrayList<?>) meta.get("statuses")).size())
                        // cartesian + NO_DATA
                        .isEqualTo(2));
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
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
                .receivesSuccess(meta -> Assertions.assertThat(((ArrayList<?>) meta.get("statuses")).size())
                        // performance + no data
                        .isEqualTo(2));
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldReturnSingleRepeatedRelationshipWarning(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {
        connection.send(wire.hello());
        connection.send(wire.logon());
        connection.send(wire.run("MATCH ()-[r]-()-[r]-() RETURN r AS r")).send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
        // Then
        assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(((ArrayList<?>) meta.get("statuses")).size())
                        // warning + no data_unknown
                        .isEqualTo(2));
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldSendFailureWithUnknownSeverity(BoltWire wire, @VersionSelected TransportConnection connection)
            throws Throwable {
        connection.send(wire.hello(x -> x.withUnknownSeverity("WANING")));

        assertThat(connection).receivesFailure();
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldSendFailureWithUnknownClassification(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {
        connection.send(wire.hello(x -> x.withUnknownDisabledCategories(List.of("Pete"))));

        assertThat(connection).receivesFailure();
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldNotReturnOnlyGeneralStatusWhenAllDisabled(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {

        connection.send(wire.hello(NotificationsMessageBuilder::withDisabledNotifications));
        connection.send(wire.logon());
        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
        assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(((ArrayList<?>) meta.get("statuses")).size())
                        // success - omitted result
                        .isEqualTo(1));
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldReturnMultipleStatuses(BoltWire wire, @VersionSelected TransportConnection connection)
            throws Throwable {

        connection.send(wire.hello());
        connection.send(wire.logon());
        connection
                .send(wire.run("MATCH (a:Person) CALL () { MATCH (a:Label) RETURN a AS aLabel } RETURN a"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);

        assertThat(connection).receivesSuccess(x -> {
            Assertions.assertThat(x).containsKey("statuses");
            Assertions.assertThat((ArrayList<?>) x.get("statuses")).hasSize(3);
        });
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldEnableNotificationsForQuery(BoltWire wire, @VersionSelected TransportConnection connection)
            throws Throwable {
        connection.send(wire.hello(NotificationsMessageBuilder::withDisabledNotifications));
        connection.send(wire.logon());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess(2);

        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());
        assertThat(connection).receivesSuccess().receivesSuccess(meta -> Assertions.assertThat(
                        ((ArrayList<?>) meta.get("statuses")).size())
                // success - omitted result
                .isEqualTo(1));
        connection
                .send(wire.run(
                        "EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)",
                        x -> x.withSeverity(NotificationConfiguration.Severity.INFORMATION)))
                .send(wire.pull());

        assertThat(connection)
                .receivesSuccess()
                .receivesSuccessWithStatus(
                        GqlStatusInfoCodes.STATUS_01N50,
                        "The label `THIS_IS_NOT_A_LABEL` does not exist. Verify that the spelling is correct.",
                        "One of the labels in your query is not available in the database, "
                                + "make sure you didn't misspell it or that the label is available when "
                                + "you run this statement in your application (the missing label name is: "
                                + "THIS_IS_NOT_A_LABEL)",
                        "The provided label is not in the database.",
                        "Neo.ClientNotification.Statement.UnknownLabelWarning",
                        BoltConnectionAssertions.assertDiagnosticRecord(
                                SeverityLevel.WARNING,
                                NotificationCategory.UNRECOGNIZED,
                                Map.of("label", "THIS_IS_NOT_A_LABEL"),
                                diagnosticRecordPosition(18L, 1L, 17L)));
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldSendFailureOnRunWithUnknownSeverity(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {
        connection.send(wire.hello(NotificationsMessageBuilder::withDisabledNotifications));
        connection.send(wire.logon());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess(2);

        connection.send(wire.run("RETURN 1 as n", x -> x.withUnknownSeverity("boom")));

        assertThat(connection).receivesFailure();
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldSendFailureOnRunWithUnknownClassification(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {

        connection.send(wire.hello(NotificationsMessageBuilder::withDisabledNotifications));
        connection.send(wire.logon());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess(2);

        connection.send(wire.run("RETURN 1 as n", x -> x.withUnknownDisabledCategories(List.of("boom"))));

        assertThat(connection).receivesFailure();
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldEnableNotificationsForQueryUsingClassifications(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {

        connection.send(wire.hello(NotificationsMessageBuilder::withDisabledNotifications));
        connection.send(wire.logon());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess(2);

        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());
        assertThat(connection).receivesSuccess().receivesSuccess(meta -> Assertions.assertThat(
                        ((ArrayList<?>) meta.get("statuses")).size())
                // success - omitted result
                .isEqualTo(1));
        ;

        connection
                .send(wire.run(
                        "EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)",
                        x -> x.withDisabledCategories(Collections.emptyList())))
                .send(wire.pull());

        assertThat(connection)
                .receivesSuccess()
                .receivesSuccessWithStatus(
                        GqlStatusInfoCodes.STATUS_01N50,
                        "The label `THIS_IS_NOT_A_LABEL` does not exist. Verify that the spelling is correct.",
                        "One of the labels in your query is not available in the database, "
                                + "make sure you didn't misspell it or that the label is available when "
                                + "you run this statement in your application (the missing label name is: "
                                + "THIS_IS_NOT_A_LABEL)",
                        "The provided label is not in the database.",
                        "Neo.ClientNotification.Statement.UnknownLabelWarning",
                        BoltConnectionAssertions.assertDiagnosticRecord(
                                SeverityLevel.WARNING,
                                NotificationCategory.UNRECOGNIZED,
                                Map.of("label", "THIS_IS_NOT_A_LABEL"),
                                diagnosticRecordPosition(18L, 1L, 17L)));
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldEnableNotificationsInBegin(BoltWire wire, @VersionSelected TransportConnection connection)
            throws Throwable {

        connection.send(wire.hello(NotificationsMessageBuilder::withDisabledNotifications));
        connection.send(wire.logon());
        assertThat(connection).receivesSuccess(2);

        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());
        assertThat(connection).receivesSuccess().receivesSuccess(meta -> Assertions.assertThat(
                        ((ArrayList<?>) meta.get("statuses")).size())
                // success - omitted result
                .isEqualTo(1));

        connection.send(wire.begin(x -> x.withSeverity(NotificationConfiguration.Severity.WARNING)));
        assertThat(connection).receivesSuccess();

        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());

        assertThat(connection)
                .receivesSuccess()
                .receivesSuccessWithStatus(
                        GqlStatusInfoCodes.STATUS_01N50,
                        "The label `THIS_IS_NOT_A_LABEL` does not exist. Verify that the spelling is correct.",
                        "One of the labels in your query is not available in the database, "
                                + "make sure you didn't misspell it or that the label is available when "
                                + "you run this statement in your application (the missing label name is: "
                                + "THIS_IS_NOT_A_LABEL)",
                        "The provided label is not in the database.",
                        "Neo.ClientNotification.Statement.UnknownLabelWarning",
                        BoltConnectionAssertions.assertDiagnosticRecord(
                                SeverityLevel.WARNING,
                                NotificationCategory.UNRECOGNIZED,
                                Map.of("label", "THIS_IS_NOT_A_LABEL"),
                                diagnosticRecordPosition(18L, 1L, 17L)));
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldEnableNotificationsInBeginWithClassifications(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {

        connection.send(wire.hello(NotificationsMessageBuilder::withDisabledNotifications));
        connection.send(wire.logon());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess(2);

        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());
        assertThat(connection).receivesSuccess().receivesSuccess(meta -> Assertions.assertThat(
                        ((ArrayList<?>) meta.get("statuses")).size())
                // success - omitted result
                .isEqualTo(1));

        connection.send(wire.begin(x -> x.withDisabledCategories(Collections.emptyList())));
        assertThat(connection).receivesSuccess();

        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());

        assertThat(connection)
                .receivesSuccess()
                .receivesSuccessWithStatus(
                        GqlStatusInfoCodes.STATUS_01N50,
                        "The label `THIS_IS_NOT_A_LABEL` does not exist. Verify that the spelling is correct.",
                        "One of the labels in your query is not available in the database, "
                                + "make sure you didn't misspell it or that the label is available when "
                                + "you run this statement in your application (the missing label name is: "
                                + "THIS_IS_NOT_A_LABEL)",
                        "The provided label is not in the database.",
                        "Neo.ClientNotification.Statement.UnknownLabelWarning",
                        BoltConnectionAssertions.assertDiagnosticRecord(
                                SeverityLevel.WARNING,
                                NotificationCategory.UNRECOGNIZED,
                                Map.of("label", "THIS_IS_NOT_A_LABEL"),
                                diagnosticRecordPosition(18L, 1L, 17L)));
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldNotReturnNotificationsInDisabledClassifications(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {
        connection.send(wire.hello(x -> x.withDisabledCategories(
                List.of(NotificationConfiguration.Category.GENERIC, NotificationConfiguration.Category.UNRECOGNIZED))));
        connection.send(wire.logon());
        connection
                .send(wire.run("MATCH (a:Person) CALL () { MATCH (a:Label) RETURN a AS aLabel } RETURN a"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);

        assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(((ArrayList<?>) meta.get("statuses")).size())
                        // NO DATA
                        .isEqualTo(1));
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 4, range = 4), @Version(major = 4)})
    public void shouldNotReturnStatusWhenNotHighEnoughSeverity(
            BoltWire wire, @VersionSelected TransportConnection connection) throws Throwable {
        connection.send(wire.hello(x -> x.withSeverity(NotificationConfiguration.Severity.WARNING)));
        connection.send(wire.logon());
        connection
                .send(wire.run("MATCH (a:Person) CALL { MATCH (a:Label) RETURN a AS aLabel } RETURN a"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);

        assertThat(connection)
                .receivesSuccess(
                        x -> Assertions.assertThat(x.get("statuses").toString()).doesNotContain("GENERIC"));
    }
}
