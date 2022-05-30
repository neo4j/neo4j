/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.testing.assertions;

import static org.neo4j.util.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.function.Consumer;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.neo4j.bolt.protocol.common.message.response.FailureMessage;
import org.neo4j.bolt.protocol.common.message.response.IgnoredMessage;
import org.neo4j.bolt.protocol.common.message.response.ResponseMessage;
import org.neo4j.bolt.protocol.common.message.response.SuccessMessage;
import org.neo4j.bolt.testing.response.RecordedMessage;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

public final class ResponseRecorderAssertions extends AbstractAssert<ResponseRecorderAssertions, ResponseRecorder> {

    private ResponseRecorderAssertions(ResponseRecorder responseRecorder) {
        super(responseRecorder, ResponseRecorderAssertions.class);
    }

    public static ResponseRecorderAssertions assertThat(ResponseRecorder recorder) {
        return new ResponseRecorderAssertions(recorder);
    }

    public static InstanceOfAssertFactory<ResponseRecorder, ResponseRecorderAssertions> responseRecorder() {
        return new InstanceOfAssertFactory<>(ResponseRecorder.class, ResponseRecorderAssertions::new);
    }

    public ResponseRecorderAssertions hasRemainingResponses(int n) {
        this.isNotNull();

        if (this.actual.remaining() != n) {
            failWithActualExpectedAndMessage(
                    this.actual.remaining(),
                    n,
                    "Expected <%d> responses to be present but got <%d>",
                    n,
                    this.actual.remaining());
        }

        return this;
    }

    public ResponseRecorderAssertions hasExactlyOneRemainingResponse() {
        return this.hasRemainingResponses(1);
    }

    public ResponseRecorderAssertions hasNoRemainingResponses() {
        return this.hasRemainingResponses(0);
    }

    public ResponseRecorderAssertions hasMessage(Consumer<RecordedMessage> assertions) {
        this.isNotNull();

        RecordedMessage message;
        try {
            message = this.actual.next();
        } catch (InterruptedException ex) {
            throw new AssertionError("Interrupted while awaiting response", ex);
        }

        assertions.accept(message);
        return this;
    }

    public ResponseRecorderAssertions hasResponseMessage(Consumer<ResponseMessage> assertions) {
        return this.hasMessage(message -> {
            if (!message.isResponse()) {
                this.failWithMessage("Expected response message but got <%s>", message);
            }

            assertions.accept(message.asResponse());
        });
    }

    public ResponseRecorderAssertions hasIgnoredResponse() {
        return this.hasResponseMessage(actual -> {
            if (actual != IgnoredMessage.INSTANCE) {
                failWithActualExpectedAndMessage(
                        actual,
                        IgnoredMessage.INSTANCE,
                        "Expected response <%s> but got <%s>",
                        IgnoredMessage.INSTANCE,
                        actual);
            }
        });
    }

    public ResponseRecorderAssertions hasIgnoredResponse(int n) {
        checkArgument(n > 0, "n must be positive");

        for (var i = 0; i < n; ++i) {
            this.hasIgnoredResponse();
        }

        return this;
    }

    public ResponseRecorderAssertions hasSuccessResponse() {
        return this.hasResponseMessage(actual -> {
            if (!(actual instanceof SuccessMessage)) {
                failWithMessage("Expected SUCCESS response but got <%s>", actual);
            }
        });
    }

    public ResponseRecorderAssertions hasSuccessResponse(int n) {
        checkArgument(n > 0, "n must be positive");

        for (var i = 0; i < n; ++i) {
            this.hasSuccessResponse();
        }

        return this;
    }

    public ResponseRecorderAssertions hasSuccessResponse(Consumer<MapValue> assertions) {
        return this.hasResponseMessage(actual -> {
            if (!(actual instanceof SuccessMessage)) {
                failWithMessage("Expected SUCCESS response but got <%s>", actual);
                return;
            }

            assertions.accept(((SuccessMessage) actual).meta());
        });
    }

    public ResponseRecorderAssertions hasFailureResponse() {
        return this.hasResponseMessage(actual -> {
            if (!(actual instanceof FailureMessage)) {
                failWithMessage("Expected FAILURE response but got <%s>", actual);
            }
        });
    }

    public ResponseRecorderAssertions hasFailureResponse(int n) {
        checkArgument(n > 0, "n must be positive");

        for (var i = 0; i < n; ++i) {
            this.hasFailureResponse();
        }

        return this;
    }

    public ResponseRecorderAssertions hasFailureResponse(Consumer<FailureMessage> assertions) {
        return this.hasResponseMessage(actual -> {
            if (!(actual instanceof FailureMessage)) {
                failWithMessage("Expected FAILURE response but got <%s>", actual);
                return;
            }

            assertions.accept((FailureMessage) actual);
        });
    }

    public ResponseRecorderAssertions hasFailureResponse(Status status) {
        return this.hasFailureResponse(msg -> {
            if (msg.status() != status) {
                failWithActualExpectedAndMessage(
                        msg.status(), status, "Expected FAILURE with status <%s> but got <%s>", status, msg.status());
            }
        });
    }

    public ResponseRecorderAssertions hasFailureResponse(String message) {
        return this.hasFailureResponse(msg -> {
            if (!message.equals(msg.message())) {
                failWithActualExpectedAndMessage(
                        msg.message(),
                        message,
                        "Expected FAILURE with message <\"%s\"> but got <\"%s\">",
                        message,
                        msg.message());
            }
        });
    }

    public ResponseRecorderAssertions hasFailureResponse(Status status, String message) {
        return this.hasFailureResponse(msg -> {
            if (msg.status() != status) {
                failWithActualExpectedAndMessage(
                        msg.status(), status, "Expected FAILURE with status <%s> but got <%s>", status, msg.status());
            }

            if (!message.equals(msg.message())) {
                failWithActualExpectedAndMessage(
                        msg.message(),
                        message,
                        "Expected FAILURE with message <\"%s\"> but got <\"%s\">",
                        message,
                        msg.message());
            }
        });
    }

    public ResponseRecorderAssertions hasRecord(Consumer<AnyValue[]> assertions) {
        return this.hasMessage(message -> {
            if (!message.isRecord()) {
                this.failWithMessage("Expected record but got <%s>", message);
            }

            assertions.accept(message.asRecord());
        });
    }

    public ResponseRecorderAssertions hasRecord() {
        return hasRecord(actual -> {});
    }

    public ResponseRecorderAssertions hasRecord(AnyValue... expected) {
        return hasRecord(actual -> {
            if (!Arrays.equals(expected, actual)) {
                failWithActualExpectedAndMessage(
                        actual,
                        expected,
                        "Expected RECORD with values <%s> but got <%s>",
                        Arrays.toString(expected),
                        Arrays.toString(actual));
            }
        });
    }

    public ResponseRecorderAssertions hasRecords(int n) {
        for (var i = 0; i < n; ++i) {
            this.hasRecord();
        }

        return this;
    }
}
