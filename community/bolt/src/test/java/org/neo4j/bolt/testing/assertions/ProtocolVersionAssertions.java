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

import java.util.List;
import java.util.function.Consumer;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.neo4j.bolt.negotiation.version.ProtocolVersion;

public final class ProtocolVersionAssertions extends AbstractAssert<ProtocolVersionAssertions, ProtocolVersion> {

    private ProtocolVersionAssertions(ProtocolVersion protocolVersion) {
        super(protocolVersion, ProtocolVersionAssertions.class);
    }

    public static ProtocolVersionAssertions assertThat(ProtocolVersion protocolVersion) {
        return new ProtocolVersionAssertions(protocolVersion);
    }

    public static InstanceOfAssertFactory<ProtocolVersion, ProtocolVersionAssertions> protocolVersion() {
        return new InstanceOfAssertFactory<>(ProtocolVersion.class, ProtocolVersionAssertions::new);
    }

    public ProtocolVersionAssertions hasMajor(int expected) {
        return this.hasMajor((short) expected);
    }

    public ProtocolVersionAssertions hasMajor(short expected) {
        this.isNotNull();

        if (this.actual.major() != expected) {
            this.failWithActualExpectedAndMessage(
                    this.actual.major(),
                    expected,
                    "Expected protocol version with major <%d> but was <%d>",
                    this.actual.major(),
                    expected);
        }

        return this;
    }

    public ProtocolVersionAssertions hasMinor(int expected) {
        return this.hasMinor((short) expected);
    }

    public ProtocolVersionAssertions hasMinor(short expected) {
        this.isNotNull();

        if (this.actual.minor() != expected) {
            this.failWithActualExpectedAndMessage(
                    this.actual.minor(),
                    expected,
                    "Expected protocol version with minor <%d> but was <%d>",
                    this.actual.minor(),
                    expected);
        }

        return this;
    }

    public ProtocolVersionAssertions hasRange() {
        this.isNotNull();

        if (!this.actual.hasRange()) {
            this.failWithMessage("Expected protocol version with range but got singular version <%s>", this.actual);
        }

        return this;
    }

    public ProtocolVersionAssertions representsSingleVersion() {
        this.isNotNull();

        if (this.actual.hasRange()) {
            this.failWithMessage("Expected singular protocol version but got ranged version <%s>", this.actual);
        }

        return this;
    }

    public ProtocolVersionAssertions hasRange(int expected) {
        return this.hasRange((short) expected);
    }

    public ProtocolVersionAssertions hasRange(short expected) {
        this.isNotNull();

        if (this.actual.range() != expected) {
            this.failWithActualExpectedAndMessage(
                    this.actual.range(),
                    expected,
                    "Expected protocol version with range <%d> but was <%d>",
                    this.actual.range(),
                    expected);
        }

        return this;
    }

    public ProtocolVersionAssertions isNegotiationVersion() {
        this.isNotNull();

        if (!this.actual.isNegotiationVersion()) {
            this.failWithMessage("Expected protocol negotiation version but got protocol version <%s>", this.actual);
        }

        return this;
    }

    public ProtocolVersionAssertions isProtocolVersion() {
        this.isNotNull();

        if (this.actual.isNegotiationVersion()) {
            this.failWithMessage("Expected protocol version but got negotiation version <%s>", this.actual);
        }

        return this;
    }

    public ProtocolVersionAssertions matches(ProtocolVersion expected) {
        this.isNotNull();

        if (!this.actual.matches(expected)) {
            this.failWithMessage("Expected protocol version <%s> to match <%s>", this.actual, expected);
        }

        return this;
    }

    public ProtocolVersionAssertions doesNotMatch(ProtocolVersion expected) {
        this.isNotNull();

        if (this.actual.matches(expected)) {
            this.failWithMessage("Did not expect protocol version <%s> to match <%s>", this.actual, expected);
        }

        return this;
    }

    public ProtocolVersionAssertions isAtLeast(ProtocolVersion expected) {
        this.isNotNull();

        if (!this.actual.isAtLeast(expected)) {
            this.failWithMessage(
                    "Expected protocol version <%s> to be equal to or newer than <%s>", this.actual, expected);
        }

        return this;
    }

    public ProtocolVersionAssertions isAtMost(ProtocolVersion expected) {
        this.isNotNull();

        if (!this.actual.isAtMost(expected)) {
            this.failWithMessage(
                    "Expected protocol version <%s> to be equal to or older than <%s>", this.actual, expected);
        }

        return this;
    }

    public ProtocolVersionAssertions hasEncoding(int expected) {
        this.isNotNull();

        if (this.actual.encode() != expected) {
            this.failWithActualExpectedAndMessage(
                    this.actual.encode(),
                    expected,
                    "Expected protocol version with encoding <0x%08X> but got <0x%08X>",
                    expected,
                    this.actual.encode());
        }

        return this;
    }

    public ProtocolVersionAssertions unwoundListSatisfies(Consumer<List<ProtocolVersion>> assertions) {
        this.isNotNull();

        var actual = this.actual.unwind();
        assertions.accept(actual);

        return this;
    }
}
