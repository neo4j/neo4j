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

import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.kernel.api.exceptions.Status;

public final class ErrorAssertions extends AbstractAssert<ErrorAssertions, Error> {

    private ErrorAssertions(Error actual) {
        super(actual, ErrorAssertions.class);
    }

    public static ErrorAssertions assertThat(Error actual) {
        return new ErrorAssertions(actual);
    }

    public static InstanceOfAssertFactory<Error, ErrorAssertions> error() {
        return new InstanceOfAssertFactory<>(Error.class, ErrorAssertions::new);
    }

    public ErrorAssertions hasStatus(Status expected) {
        this.isNotNull();

        if (this.actual.status() != expected) {
            this.failWithActualExpectedAndMessage(
                    this.actual.status(),
                    expected,
                    "Expected status <%s> but got <%s>",
                    expected,
                    this.actual.status());
        }

        return this;
    }

    public ErrorAssertions hasMessage(String expected) {
        this.isNotNull();

        if (!Objects.equals(expected, this.actual.message())) {
            this.failWithActualExpectedAndMessage(
                    this.actual.message(),
                    expected,
                    "Expected message <\"%s\"> but got <\"%s\">",
                    expected,
                    this.actual.message());
        }

        return this;
    }

    public ErrorAssertions hasMessageContaining(String substring) {
        this.isNotNull();

        var actual = this.actual.message();
        if (actual == null || !actual.contains(substring)) {
            this.failWithActualExpectedAndMessage(
                    actual,
                    substring,
                    "Expected message to contain substring <\"%s\"> but got <\"%s\">",
                    substring,
                    actual);
        }

        return this;
    }

    public ErrorAssertions hasCause(Consumer<Throwable> assertions) {
        this.isNotNull();

        assertions.accept(this.actual.cause());

        return this;
    }

    public ErrorAssertions hasCauseInstanceOf(Class<? extends Throwable> expected) {
        this.isNotNull();

        if (!expected.isInstance(this.actual.cause())) {
            var actual = this.actual.cause();

            Class<?> actualType = null;
            String actualTypeName = "null";
            if (actual != null) {
                actualType = actual.getClass();
                actualTypeName = actualType.getName();
            }

            this.failWithActualExpectedAndMessage(
                    actualType,
                    expected,
                    "Expected cause of type <%s> but got <%s>",
                    expected.getName(),
                    actualTypeName);
        }

        return this;
    }

    public ErrorAssertions hasNoCause() {
        this.isNotNull();

        if (this.actual.cause() != null) {
            this.failWithMessage("Expected no cause to be present");
        }

        return this;
    }

    public ErrorAssertions hasReference(UUID expected) {
        this.isNotNull();

        if (!Objects.equals(expected, this.actual.reference())) {
            this.failWithActualExpectedAndMessage(
                    this.actual.reference(),
                    expected,
                    "Expected reference <%s> but got <%s>",
                    expected,
                    this.actual.reference());
        }

        return this;
    }

    public ErrorAssertions hasQueryId(Long expected) {
        this.isNotNull();

        if (!Objects.equals(expected, this.actual.queryId())) {
            this.failWithActualExpectedAndMessage(
                    this.actual.queryId(),
                    expected,
                    "Expected queryId <%s> but got <%s>",
                    expected,
                    this.actual.queryId());
        }

        return this;
    }

    public ErrorAssertions isFatal() {
        this.isNotNull();

        if (!this.actual.isFatal()) {
            this.failWithMessage("Expected error to be fatal");
        }

        return this;
    }

    public ErrorAssertions isNotFatal() {
        this.isNotNull();

        if (this.actual.isFatal()) {
            this.failWithMessage("Expected error to not be fatal");
        }

        return this;
    }
}
