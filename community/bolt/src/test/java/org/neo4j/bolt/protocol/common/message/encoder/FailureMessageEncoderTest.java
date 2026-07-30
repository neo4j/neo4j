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
package org.neo4j.bolt.protocol.common.message.encoder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.boltmessages.response.FailureMessage;
import org.neo4j.boltmessages.response.FailureMetadata;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.testing.PackstreamBufAssertions;

class FailureMessageEncoderTest {

    @Test
    void getInstance() {
        assertInstanceOf(FailureMessageEncoder.class, FailureMessageEncoder.getInstance());
    }

    @Test
    void getType() {
        var encoder = FailureMessageEncoder.getInstance();

        assertEquals(FailureMessage.class, encoder.getType());
    }

    @Test
    void getTag() {
        var encoder = FailureMessageEncoder.getInstance();

        assertEquals(message().signature(), encoder.getTag(message()));
    }

    @Test
    void getLength() {
        var encoder = FailureMessageEncoder.getInstance();

        assertEquals(1, encoder.getLength(message()));
    }

    @ParameterizedTest
    @MethodSource("failureMessageAndExpectedMetadata")
    void write(FailureMessage message, Map<String, Object> expectedMetadata) {
        var buf = PackstreamBuf.allocUnpooled();
        var encoder = FailureMessageEncoder.getInstance();

        encoder.write(null, buf, message);

        PackstreamBufAssertions.assertThat(buf)
                .containsMap(meta -> assertThat(meta).isNotNull().isEqualTo(expectedMetadata));

        assertThat(buf.raw().isReadable()).isFalse();
    }

    private static List<Arguments> failureMessageAndExpectedMetadata() {
        var sourceFailures = List.of(
                failureWithNoCauseAndDefaultDiagnosticRecord(),
                failureWithNoCauseAndDefaultDiagnosticRecordButStatusParams(),
                failureWithNoCauseAndDefaultDiagnosticRecordButClassification(),
                failureWithNoCauseAndDefaultDiagnosticRecordButPosition(),
                failureWithNoCauseAndDefaultDiagnosticRecordButDefaultPosition(),
                failureWithNoCauseAndDefaultDiagnosticRecordButMadeUpRecord(),
                failureWithNoCauseAndNonDefaultDiagnosticRecord());

        var argumentsList = new ArrayList<Arguments>();

        for (var failure : sourceFailures) {
            argumentsList.add(Arguments.of(failure.getLeft(), failure.getRight()));
            for (var cause : sourceFailures) {
                var failureWithCause = copyWithCause(failure, cause);
                argumentsList.add(Arguments.of(failureWithCause.getLeft(), failureWithCause.getRight()));
            }
        }

        return argumentsList;
    }

    private static Pair<FailureMessage, Map<String, Object>> failureWithNoCauseAndDefaultDiagnosticRecord() {
        return Pair.of(
                new FailureMessage(
                        new FailureMetadata(
                                Status.General.UnknownError,
                                "50N42: something, unknown error but in gql",
                                "Unknown error, mate",
                                "error: something, unknown error but in gql",
                                "50N42",
                                Map.of(
                                        "OPERATION",
                                        "",
                                        "OPERATION_CODE",
                                        "0",
                                        "CURRENT_SCHEMA",
                                        "/",
                                        "_position",
                                        Map.of(
                                                "offset", -1,
                                                "line", -1,
                                                "column", -1)),
                                null),
                        false),
                Map.of(
                        "neo4j_code", Status.General.UnknownError.code().serialize(),
                        "message", "Unknown error, mate",
                        "description", "error: something, unknown error but in gql",
                        "gql_status", "50N42"));
    }

    private static Pair<FailureMessage, Map<String, Object>>
            failureWithNoCauseAndDefaultDiagnosticRecordButStatusParams() {
        return Pair.of(
                new FailureMessage(
                        new FailureMetadata(
                                Status.General.UnknownError,
                                "50N42: something, unknown error but in gql",
                                "Unknown error, mate",
                                "error: something, unknown error but in gql",
                                "50N42",
                                Map.of(
                                        "OPERATION",
                                        "",
                                        "OPERATION_CODE",
                                        "0",
                                        "CURRENT_SCHEMA",
                                        "/",
                                        "_status_parameters",
                                        Map.of(
                                                "param1",
                                                1,
                                                "param2",
                                                "2",
                                                "paramTrue",
                                                true,
                                                "paramListOfString",
                                                List.of("b", "c"))),
                                null),
                        false),
                Map.of(
                        "neo4j_code",
                        Status.General.UnknownError.code().serialize(),
                        "message",
                        "Unknown error, mate",
                        "description",
                        "error: something, unknown error but in gql",
                        "gql_status",
                        "50N42",
                        "diagnostic_record",
                        Map.of(
                                "_status_parameters",
                                Map.of(
                                        "param1",
                                        1L,
                                        "param2",
                                        "2",
                                        "paramTrue",
                                        true,
                                        "paramListOfString",
                                        List.of("b", "c")))));
    }

    private static Pair<FailureMessage, Map<String, Object>>
            failureWithNoCauseAndDefaultDiagnosticRecordButClassification() {
        return Pair.of(
                new FailureMessage(
                        new FailureMetadata(
                                Status.General.UnknownError,
                                "50N42: something, unknown error but in gql",
                                "Unknown error, mate",
                                "error: something, unknown error but in gql",
                                "50N42",
                                Map.of(
                                        "OPERATION",
                                        "",
                                        "OPERATION_CODE",
                                        "0",
                                        "CURRENT_SCHEMA",
                                        "/",
                                        "_classification",
                                        "CLIENT"),
                                null),
                        false),
                Map.of(
                        "neo4j_code",
                        Status.General.UnknownError.code().serialize(),
                        "message",
                        "Unknown error, mate",
                        "description",
                        "error: something, unknown error but in gql",
                        "gql_status",
                        "50N42",
                        "diagnostic_record",
                        Map.of("_classification", "CLIENT")));
    }

    private static Pair<FailureMessage, Map<String, Object>> failureWithNoCauseAndDefaultDiagnosticRecordButPosition() {
        return Pair.of(
                new FailureMessage(
                        new FailureMetadata(
                                Status.General.UnknownError,
                                "50N42: something, unknown error but in gql",
                                "Unknown error, mate",
                                "error: something, unknown error but in gql",
                                "50N42",
                                Map.of(
                                        "OPERATION",
                                        "",
                                        "OPERATION_CODE",
                                        "0",
                                        "CURRENT_SCHEMA",
                                        "/",
                                        "_position",
                                        Map.of(
                                                "offset", 4,
                                                "line", 1,
                                                "column", -6)),
                                null),
                        false),
                Map.of(
                        "neo4j_code",
                        Status.General.UnknownError.code().serialize(),
                        "message",
                        "Unknown error, mate",
                        "description",
                        "error: something, unknown error but in gql",
                        "gql_status",
                        "50N42",
                        "diagnostic_record",
                        Map.of(
                                "_position",
                                Map.of(
                                        "offset", 4L,
                                        "line", 1L,
                                        "column", -6L))));
    }

    private static Pair<FailureMessage, Map<String, Object>>
            failureWithNoCauseAndDefaultDiagnosticRecordButDefaultPosition() {
        return Pair.of(
                new FailureMessage(
                        new FailureMetadata(
                                Status.General.UnknownError,
                                "50N42: something, unknown error but in gql",
                                "Unknown error, mate",
                                "error: something, unknown error but in gql",
                                "50N42",
                                Map.of(
                                        "OPERATION",
                                        "",
                                        "OPERATION_CODE",
                                        "0",
                                        "CURRENT_SCHEMA",
                                        "/",
                                        "_position",
                                        Map.of(
                                                "offset", -1,
                                                "line", -1,
                                                "column", -1)),
                                null),
                        false),
                Map.of(
                        "neo4j_code",
                        Status.General.UnknownError.code().serialize(),
                        "message",
                        "Unknown error, mate",
                        "description",
                        "error: something, unknown error but in gql",
                        "gql_status",
                        "50N42"));
    }

    private static Pair<FailureMessage, Map<String, Object>>
            failureWithNoCauseAndDefaultDiagnosticRecordButMadeUpRecord() {
        return Pair.of(
                new FailureMessage(
                        new FailureMetadata(
                                Status.General.UnknownError,
                                "50N42: something, unknown error but in gql",
                                "Unknown error, mate",
                                "error: something, unknown error but in gql",
                                "50N42",
                                Map.of("OPERATION", "", "OPERATION_CODE", "0", "CURRENT_SCHEMA", "/", "_made", "UP"),
                                null),
                        false),
                Map.of(
                        "neo4j_code",
                        Status.General.UnknownError.code().serialize(),
                        "message",
                        "Unknown error, mate",
                        "description",
                        "error: something, unknown error but in gql",
                        "gql_status",
                        "50N42",
                        "diagnostic_record",
                        Map.of("_made", "UP")));
    }

    private static Pair<FailureMessage, Map<String, Object>> failureWithNoCauseAndNonDefaultDiagnosticRecord() {
        return Pair.of(
                new FailureMessage(
                        new FailureMetadata(
                                Status.General.UnknownError,
                                "50N42: something, unknown error but in gql",
                                "Unknown error, mate",
                                "error: something, unknown error but in gql",
                                "50N42",
                                Map.of("OPERATION", "Nope", "OPERATION_CODE", "01", "CURRENT_SCHEMA", "/neo"),
                                null),
                        false),
                Map.of(
                        "neo4j_code",
                        Status.General.UnknownError.code().serialize(),
                        "message",
                        "Unknown error, mate",
                        "description",
                        "error: something, unknown error but in gql",
                        "gql_status",
                        "50N42",
                        "diagnostic_record",
                        Map.of("OPERATION", "Nope", "OPERATION_CODE", "01", "CURRENT_SCHEMA", "/neo")));
    }

    private static Pair<FailureMessage, Map<String, Object>> copyWithCause(
            Pair<FailureMessage, Map<String, Object>> source, Pair<FailureMessage, Map<String, Object>> cause) {
        var sourceFailure = source.getLeft();
        var causeFailure = cause.getLeft();
        var failure = new FailureMessage(
                new FailureMetadata(
                        sourceFailure.metadata().status(),
                        sourceFailure.metadata().message(),
                        sourceFailure.metadata().legacyMessage(),
                        sourceFailure.metadata().description(),
                        sourceFailure.metadata().gqlStatus(),
                        sourceFailure.metadata().diagnosticRecord(),
                        causeFailure.metadata()),
                sourceFailure.fatal());

        var failureMap = new HashMap<String, Object>();
        failureMap.putAll(source.getRight());
        var causeMap = cause.getRight().entrySet().stream()
                // Causes doesn't have neo4j_code
                .filter(e -> !e.getKey().equals("neo4j_code"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        causeMap.put("message", causeFailure.metadata().message());
        failureMap.put("cause", causeMap);
        return Pair.of(failure, failureMap);
    }

    private static FailureMessage message() {
        return new FailureMessage(
                new FailureMetadata(
                        Status.Request.InvalidFormat,
                        "50N42: some wrong, but in gql phrasing",
                        "Something wrong",
                        "some wrong, but in gql phrasing",
                        "50N52",
                        Map.of("OPERATION", "", "OPERATION_CODE", "0", "CURRENT_SCHEMA", "/"),
                        new FailureMetadata(
                                Status.Request.InvalidFormat,
                                "50N42: some wrong, but in gql phrasing",
                                "Something wrong",
                                "some wrong, but in gql phrasing",
                                "50N52",
                                Map.of("OPERATION", "", "OPERATION_CODE", "0", "CURRENT_SCHEMA", "/"),
                                null)),
                false);
    }
}
