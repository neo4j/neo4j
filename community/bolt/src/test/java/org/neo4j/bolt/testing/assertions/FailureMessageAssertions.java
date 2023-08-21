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

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.neo4j.bolt.protocol.common.message.response.FailureMessage;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.Status.Classification;

public class FailureMessageAssertions extends ResponseMessageAssertions<FailureMessageAssertions, FailureMessage> {

    private FailureMessageAssertions(FailureMessage actual) {
        super(actual, FailureMessageAssertions.class);
    }

    public static FailureMessageAssertions assertThat(FailureMessage actual) {
        return new FailureMessageAssertions(actual);
    }

    public static InstanceOfAssertFactory<FailureMessage, FailureMessageAssertions> failureMessage() {
        return new InstanceOfAssertFactory<>(FailureMessage.class, FailureMessageAssertions::assertThat);
    }

    public FailureMessageAssertions hasStatus(Status expected) {
        this.isNotNull();

        var status = this.actual.status();
        if (status != expected) {
            this.failWithActualExpectedAndMessage(
                    status,
                    expected,
                    "Expected failure with status <%s> but got <%s>",
                    expected.code().serialize(),
                    status.code().serialize());
        }

        return this;
    }

    public FailureMessageAssertions hasClassification(Classification expected) {
        this.isNotNull();

        var status = this.actual.status();
        if (status.code().classification() != expected) {
            this.failWithActualExpectedAndMessage(
                    status,
                    expected,
                    "Expected failure with classification <%s> but got <%s>",
                    expected.name(),
                    status.code().classification().name());
        }

        return this;
    }

    public FailureMessageAssertions hasMessage(String expected) {
        this.isNotNull();

        this.extracting(FailureMessage::message, Assertions::assertThat).isEqualTo(expected);

        return this;
    }

    public FailureMessageAssertions hasMessageContaining(String... expected) {
        this.isNotNull();

        this.extracting(FailureMessage::message, Assertions::assertThat).contains(expected);

        return this;
    }

    public FailureMessageAssertions isFatal() {
        this.isNotNull();

        if (!this.actual.fatal()) {
            this.failWithMessage("Expected fatal failure");
        }

        return this;
    }

    public FailureMessageAssertions isNotFatal() {
        this.isNotNull();

        if (this.actual.fatal()) {
            this.failWithMessage("Expected non-fatal failure");
        }

        return this;
    }
}
