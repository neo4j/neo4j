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
package org.neo4j.bolt.testing.assertions;

import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.neo4j.packstream.testing.PackstreamConnectionAssertions.packstreamConnection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.gqlstatus.Condition;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.graphdb.NotificationCategory;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.error.reader.LimitExceededException;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.testing.PackstreamBufAssertions;
import org.neo4j.packstream.testing.PackstreamConnectionAssertions;
import org.neo4j.packstream.testing.PackstreamTestValueReader;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import org.opentest4j.AssertionFailedError;

public final class BoltConnectionAssertions
        extends TransportConnectionAssertions<BoltConnectionAssertions, TransportConnection> {

    private BoltConnectionAssertions(TransportConnection transportConnection) {
        super(transportConnection, BoltConnectionAssertions.class);
    }

    public static BoltConnectionAssertions assertThat(TransportConnection value) {
        return new BoltConnectionAssertions(value);
    }

    public static InstanceOfAssertFactory<TransportConnection, BoltConnectionAssertions> boltConnection() {
        return new InstanceOfAssertFactory<>(TransportConnection.class, BoltConnectionAssertions::new);
    }

    public PackstreamConnectionAssertions asPackstream() {
        return this.asInstanceOf(packstreamConnection());
    }

    public BoltConnectionAssertions packstreamSatisfies(Consumer<PackstreamConnectionAssertions> assertions) {
        assertions.accept(this.asPackstream());
        return this;
    }

    public BoltConnectionAssertions hasReceivedNoopChunks() {
        var actual = this.actual.noopCount();
        if (actual == 0) {
            failWithActualExpectedAndMessage(
                    actual, 1, "Expected to receive at least one NOOP chunk but got <%d>", actual);
        }

        return this;
    }

    public BoltConnectionAssertions hasReceivedNoopChunks(long expected) {
        var actual = this.actual.noopCount();
        if (actual != expected) {
            failWithActualExpectedAndMessage(
                    actual, expected, "Expected to receive <%d> NOOP chunks but got <%d>", expected, actual);
        }

        return this;
    }

    public BoltConnectionAssertions receivesSuccess(Consumer<Map<String, Object>> assertions) {
        this.asInstanceOf(packstreamConnection())
                .receivesMessage()
                .satisfies(msg -> {
                    try {
                        PackstreamBufAssertions.assertThat(msg).containsStruct(0x70, 1);
                    } catch (AssertionFailedError ex) {
                        var actual = ex.getActual();
                        if (Short.class == actual.getType()) {
                            var type = (Short) actual.getValue();
                            if (type == (short) 0x7F) {
                                try {
                                    var meta = PackstreamTestValueReader.readMapValue(
                                            msg, PackstreamTestValueReader.DEFAULT_STRUCT_REGISTRY);
                                    throw new AssertionFailedError(
                                            ex.getMessage() + ": " + meta,
                                            ex.getExpected(),
                                            ex.getActual(),
                                            ex.getCause());
                                } catch (PackstreamReaderException ignore) {
                                }
                            }
                        }

                        throw ex;
                    }
                })
                .containsMap(assertions)
                .asBuffer()
                .hasNoRemainingReadableBytes();

        return this;
    }

    public BoltConnectionAssertions receivesSuccess() {
        return this.receivesSuccess(meta -> {
            // Nothing to do here
        });
    }

    public BoltConnectionAssertions receivesSuccess(int n) {
        for (var i = 0; i < n; ++i) {
            try {
                this.receivesSuccess();
            } catch (AssertionError ex) {
                throw new AssertionError(
                        "Failed to retrieve expected message (received " + i + " out of " + n + ")", ex);
            }
        }

        return this;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public BoltConnectionAssertions receivesSuccessWithNotifications(Consumer<List<Map<String, Object>>> assertions) {
        return this.receivesSuccess(meta -> {
            Assertions.assertThat(meta)
                    .as("contains matching notifications")
                    .hasEntrySatisfying("notifications", notifications -> {
                        Assertions.assertThat(notifications)
                                .asInstanceOf(InstanceOfAssertFactories.list(Map.class))
                                .satisfies((Consumer) assertions);
                    });
        });
    }

    public BoltConnectionAssertions receivesSuccessWithNotification(Consumer<Map<String, Object>> assertions) {
        return receivesSuccessWithNotifications(
                notifications -> Assertions.assertThat(notifications).anySatisfy(assertions));
    }

    public BoltConnectionAssertions receivesSuccessWithNotification(
            String code, String title, String description, SeverityLevel severity, int offset, int line, int column) {
        return receivesSuccessWithNotification(notification -> assertSoftly(soft -> soft.assertThat(notification)
                .as("contains notification")
                .containsEntry("code", code)
                .containsEntry("title", title)
                .containsEntry("description", description)
                .containsEntry("severity", severity.toString())
                .hasEntrySatisfying("position", position -> Assertions.assertThat(position)
                        .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                        .containsEntry("offset", (long) offset)
                        .containsEntry("line", (long) line)
                        .containsEntry("column", (long) column))));
    }

    public BoltConnectionAssertions receivesSuccessWithNotification(
            String code,
            String title,
            String description,
            SeverityLevel severity,
            NotificationCategory category,
            int offset,
            int line,
            int column) {
        return receivesSuccessWithNotification(notification -> assertSoftly(soft -> soft.assertThat(notification)
                .as("contains notification")
                .containsEntry("code", code)
                .containsEntry("title", title)
                .containsEntry("description", description)
                .containsEntry("severity", severity.toString())
                .containsEntry("category", category.toString())
                .hasEntrySatisfying("position", position -> Assertions.assertThat(position)
                        .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                        .containsEntry("offset", (long) offset)
                        .containsEntry("line", (long) line)
                        .containsEntry("column", (long) column))));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public BoltConnectionAssertions receivesSuccessWithStatuses(Consumer<List<Map<String, Object>>> assertions) {
        return this.receivesSuccess(meta -> {
            Assertions.assertThat(meta).as("contains matching statuses").hasEntrySatisfying("statuses", statuses -> {
                Assertions.assertThat(statuses)
                        .asInstanceOf(InstanceOfAssertFactories.list(Map.class))
                        .satisfies((Consumer) assertions);
            });
        });
    }

    public BoltConnectionAssertions receivesSuccessWithStatus(Consumer<Map<String, Object>> assertions) {
        return receivesSuccessWithStatuses(
                status -> Assertions.assertThat(status).anySatisfy(assertions));
    }

    public BoltConnectionAssertions receivesSuccessWithStatus(GqlStatusInfoCodes gqlStatus) {
        return receivesSuccessWithStatus(status -> assertSoftly(soft -> soft.assertThat(status)
                .as("contains status")
                .containsExactly(
                        Map.entry("gql_status", gqlStatus.getStatusString()),
                        Map.entry(
                                "status_description",
                                Condition.createStandardDescription(
                                        gqlStatus.getCondition(), gqlStatus.getSubCondition())))));
    }

    public BoltConnectionAssertions receivesSuccessWithStatus(
            GqlStatusInfoCodes gqlStatus,
            String statusMessage,
            String description,
            String title,
            String neo4jCode,
            Consumer<Map<String, Object>> diagnosticRecordAssertions) {
        return receivesSuccessWithStatus(status -> assertSoftly(soft -> soft.assertThat(status)
                .as("contains status")
                .containsOnlyKeys(
                        "gql_status", "status_description", "description", "title", "neo4j_code", "diagnostic_record")
                .containsEntry("gql_status", gqlStatus.getStatusString())
                .containsEntry(
                        "status_description",
                        Condition.createStandardDescription(gqlStatus.getCondition(), gqlStatus.getSubCondition())
                                .concat(". ")
                                .concat(statusMessage))
                .containsEntry("title", title)
                .containsEntry("description", description)
                .containsEntry("neo4j_code", neo4jCode)
                .hasEntrySatisfying("diagnostic_record", diagnosticRecord -> {
                    Assertions.assertThat(diagnosticRecord)
                            .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                            .satisfies(diagnosticRecordAssertions);
                })));
    }

    public static Consumer<Map<String, Object>> assertDiagnosticRecord(
            SeverityLevel severityLevel,
            NotificationCategory classification,
            Map<String, Object> statusParameters,
            Map<String, Long> position) {
        return diagnosticRecord -> Assertions.assertThat(diagnosticRecord)
                .containsOnlyKeys("_severity", "_classification", "_status_parameters", "_position")
                .containsEntry("_severity", severityLevel.name())
                .containsEntry("_classification", classification.name())
                .containsEntry("_status_parameters", statusParameters)
                .containsEntry("_position", position);
    }

    public static Map<String, Long> diagnosticRecordPosition(long column, long line, long offset) {
        return Map.of("column", column, "line", line, "offset", offset);
    }

    public BoltConnectionAssertions receivesIgnored() {
        this.asInstanceOf(packstreamConnection())
                .as("Receives an IGNORED response")
                .receivesMessage()
                .containsStruct(0x7E, 0)
                .asBuffer()
                .hasNoRemainingReadableBytes();

        return this;
    }

    public BoltConnectionAssertions receivesIgnored(int n) {
        for (var i = 0; i < n; ++i) {
            this.receivesIgnored();
        }

        return this;
    }

    public BoltConnectionAssertions receivesFailure(Consumer<Map<String, Object>> assertions) {
        this.asInstanceOf(packstreamConnection())
                .receivesMessage()
                .containsStruct(0x7F, 1)
                .containsMap(assertions)
                .asBuffer()
                .hasNoRemainingReadableBytes();

        return this;
    }

    public BoltConnectionAssertions receivesFailure() {
        return this.receivesFailure(meta -> {
            // NOOP
        });
    }

    public BoltConnectionAssertions receivesFailure(int n) {
        for (var i = 0; i < n; ++i) {
            this.receivesFailure();
        }

        return this;
    }

    public BoltConnectionAssertions receivesFailure(Status... statuses) {
        return this.receivesFailure(meta -> Assertions.assertThat(meta).satisfies(metaMap -> {
            var code = metaMap.get("code");
            var serializedList = Arrays.stream(statuses)
                    .map(status -> status.code().serialize())
                    .collect(Collectors.toList());
            Assertions.assertThat(code).isIn(serializedList);
        }));
    }

    public BoltConnectionAssertions receivesFailure(Status status, String message) {
        return this.receivesFailure(meta -> Assertions.assertThat(meta)
                .containsEntry("code", status.code().serialize())
                .containsEntry("message", message));
    }

    public BoltConnectionAssertions receivesFailureFuzzy(Status status, String message) {
        return this.receivesFailure(meta -> Assertions.assertThat(meta)
                .containsEntry("code", status.code().serialize())
                .hasEntrySatisfying("message", msg -> Assertions.assertThat(msg)
                        .asInstanceOf(InstanceOfAssertFactories.STRING)
                        .contains(message)));
    }

    public BoltConnectionAssertions receivesResponse() {
        this.asInstanceOf(packstreamConnection()).receivesMessage().containsStruct(struct -> Assertions.assertThat(
                        struct.tag())
                .isIn(
                        (short) 0x70, // Success
                        (short) 0x7E, // Failure
                        (short) 0x7F // Ignored
                        ));

        return this;
    }

    public BoltConnectionAssertions receivesResponse(int n) {
        for (var i = 0; i < n; ++n) {
            this.receivesResponse();
        }

        return this;
    }

    public BoltConnectionAssertions receivesResponseOrRecord() {
        this.asInstanceOf(packstreamConnection())
                .receivesMessages(buf -> buf.peek(b -> {
                    try {
                        var struct = b.readStructHeader();
                        return struct.tag() == 0x71;
                    } catch (UnexpectedTypeException ex) {
                        failWithActualExpectedAndMessage(
                                ex.getActual(),
                                ex.getExpected(),
                                "Expected Packstream type <%s> but got <%s>",
                                ex.getExpected(),
                                ex.getActual());
                        return false;
                    } catch (LimitExceededException ex) {
                        failWithActualExpectedAndMessage(
                                ex.getActual(),
                                ex.getLimit(),
                                "Packstream value exceeded limit of <%d> with value of size <%d>",
                                ex.getLimit(),
                                ex.getActual());
                        return false;
                    }
                }))
                .last() // TODO: We can't get IGNORED when at least a single RECORD has been written
                .containsStruct(struct -> Assertions.assertThat(struct.tag())
                        .isIn(
                                (short) 0x70, // Success
                                (short) 0x7E, // Failure
                                (short) 0x7F // Ignored
                                ));

        return this;
    }

    public BoltConnectionAssertions receivesResponseOrRecord(int n) {
        for (var i = 0; i < n; ++i) {
            this.receivesResponseOrRecord();
        }

        return this;
    }

    public BoltConnectionAssertions receivesRecord(Consumer<ListValue> assertions) {
        this.asInstanceOf(packstreamConnection())
                .receivesMessage()
                .containsStruct(0x71, 1)
                .containsListValue(assertions)
                .asBuffer()
                .hasNoRemainingReadableBytes();

        return this;
    }

    public BoltConnectionAssertions receivesRecord(AnyValue... values) {
        return this.receivesRecord(
                fields -> Assertions.assertThat(fields).hasSize(values.length).containsExactly(values));
    }

    public BoltConnectionAssertions receivesRecord() {
        return this.receivesRecord(record -> {
            // NOOP
        });
    }

    private BoltConnectionAssertions receivesRecords(
            Consumer<List<List<AnyValue>>> recordAssertions, Consumer<PackstreamBuf> terminationAssertions) {
        this.asInstanceOf(packstreamConnection())
                .receivesMessages(buf -> buf.peek(b -> {
                    try {
                        var header = b.readStructHeader();
                        return header.tag() == 0x71;
                    } catch (PackstreamReaderException ex) {
                        return false;
                    }
                }))
                .satisfies(messages -> {
                    var records = new ArrayList<PackstreamBuf>(messages.size() - 1);
                    for (var i = 0; i < messages.size() - 1; ++i) {
                        records.add(messages.get(i));
                    }

                    recordAssertions.accept(records.stream()
                            .map(buf -> {
                                PackstreamBufAssertions.assertThat(buf).containsStruct(0x71, 1);

                                try {
                                    return buf.readList(b -> PackstreamTestValueReader.readStorable(
                                            b, PackstreamTestValueReader.DEFAULT_STRUCT_REGISTRY));
                                } catch (PackstreamReaderException ex) {
                                    failWithMessage(
                                            "Expected record to contain valid list of fields: %s", ex.getMessage());
                                    return null;
                                }
                            })
                            .toList());

                    terminationAssertions.accept(messages.get(messages.size() - 1));
                });

        return this;
    }

    public BoltConnectionAssertions receivesRecords(Consumer<List<List<AnyValue>>> assertions) {
        return this.receivesRecords(
                assertions, buf -> PackstreamBufAssertions.assertThat(buf).containsStruct(0x70, 1));
    }

    public BoltConnectionAssertions receivesRecords() {
        this.asInstanceOf(packstreamConnection())
                .receivesMessages(buf -> buf.peek(b -> {
                    try {
                        var header = b.readStructHeader();
                        return header.tag() == 0x71;
                    } catch (PackstreamReaderException ex) {
                        return false;
                    }
                }))
                .hasSizeGreaterThan(1)
                .last()
                .containsStruct(0x70, 1)
                .containsMap(meta -> {
                    // NOOP
                })
                .asBuffer()
                .hasNoRemainingReadableBytes();

        return this;
    }

    public BoltConnectionAssertions receivesSuccessAfterRecords(Consumer<Map<String, Object>> assertions) {
        return this.receivesRecords(
                records -> {
                    // Don't care about records
                },
                buf -> PackstreamBufAssertions.assertThat(buf)
                        .containsStruct(0x70, 1)
                        .containsMap(assertions)
                        .asBuffer()
                        .hasNoRemainingReadableBytes());
    }

    public BoltConnectionAssertions receivesFailureAfterRecords(Consumer<Map<String, Object>> assertions) {
        return this.receivesRecords(
                records -> {
                    // Don't care about records
                },
                buf -> PackstreamBufAssertions.assertThat(buf)
                        .containsStruct(0x7F, 1)
                        .containsMap(assertions)
                        .asBuffer()
                        .hasNoRemainingReadableBytes());
    }

    public BoltConnectionAssertions receivesFailureAfterRecords() {
        return receivesFailureAfterRecords(meta -> {});
    }

    public BoltConnectionAssertions receivesFailureAfterRecords(Status status) {
        return this.receivesFailureAfterRecords(meta ->
                Assertions.assertThat(meta).containsEntry("code", status.code().serialize()));
    }

    public BoltConnectionAssertions receivesFailureAfterRecords(Status status, String message) {
        return this.receivesFailureAfterRecords(meta -> Assertions.assertThat(meta)
                .containsEntry("code", status.code().serialize())
                .containsEntry("message", message));
    }

    public BoltConnectionAssertions receivesAnyRecord(Consumer<List<AnyValue>> assertions) {
        return this.receivesRecords(records -> Assertions.assertThat(records).anySatisfy(assertions));
    }
}
