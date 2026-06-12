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
package org.neo4j.packstream.util;

import static org.neo4j.bolt.testing.util.ErrorUtil.useNewMessage;

import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

class PackstreamConditionsTest {

    @TestFactory
    Stream<DynamicTest> requireLengthShouldAcceptExpectedValues() {
        return IntStream.of(1, 2, 4, 5, 7, 9, 42, 84, 128, 255)
                .mapToObj(expected -> DynamicTest.dynamicTest(
                        expected + " fields",
                        () -> PackstreamConditions.requireLength(
                                new StructHeader(expected, (short) (expected * 2)), expected)));
    }

    @TestFactory
    Stream<DynamicTest> requireLengthShouldRejectMismatchingValues() {
        return IntStream.of(1, 2, 4, 5, 7, 9, 42, 84, 128, 255)
                .mapToObj(expected -> DynamicTest.dynamicTest(
                        expected + " fields",
                        () -> ErrorGqlStatusObjectAssertions.assertThatThrownBy(
                                        () -> PackstreamConditions.requireLength(
                                                new StructHeader(expected + 1, (short) (expected * 2)), expected))
                                .isInstanceOf(IllegalStructSizeException.class)
                                .hasMessage(useNewMessage(
                                                "08N11: The request is invalid and could not be processed by the server. See cause for further details.")
                                        .whenLegacyFallbackTo("Illegal struct size: Expected struct to be " + expected
                                                + " fields but got " + (expected + 1)))
                                .hasNoCause()
                                .hasGqlStatus(GqlStatusInfoCodes.STATUS_08N11)
                                .hasStatusDescription(
                                        "error: connection exception - request error. The request is invalid and could not be processed by the server. See cause for further details.")
                                .gqlCause()
                                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N57)
                                .hasStatusDescription(String.format(
                                        "error: data exception - invalid protocol type. Protocol type is invalid. Invalid number of struct components (received %s but expected %s).",
                                        expected + 1, expected))));
    }

    @TestFactory
    Stream<DynamicTest> requireEmptyShouldAcceptEmptyValues() {
        return IntStream.of(1, 2, 4, 5, 7, 9, 42, 84, 128, 255)
                .mapToObj(tag -> new StructHeader(0, (short) tag))
                .map(header ->
                        DynamicTest.dynamicTest(header.toString(), () -> PackstreamConditions.requireEmpty(header)));
    }

    @TestFactory
    Stream<DynamicTest> requireEmptyShouldRejectNonEmptyStructures() {
        return IntStream.of(1, 2, 4, 5, 7, 9, 42, 84, 128, 255)
                .mapToObj(length -> new StructHeader(length, (short) (length * 2)))
                .map(header -> DynamicTest.dynamicTest(
                        header.length() + " fields",
                        () -> Assertions.assertThatExceptionOfType(IllegalStructSizeException.class)
                                .isThrownBy(() -> PackstreamConditions.requireEmpty(header))
                                .withMessage(useNewMessage(
                                                "08N11: The request is invalid and could not be processed by the server. See cause for further details.")
                                        .whenLegacyFallbackTo(
                                                "Illegal struct size: Expected struct to be 0 fields but got "
                                                        + header.length()))
                                .withNoCause()));
    }

    @TestFactory
    Stream<DynamicTest> requireNonNullShouldAcceptNonNullValues() {
        return Stream.of("foo", "bar", "baz")
                .flatMap(fieldName -> Stream.of(42, 84L, "potato", new Object())
                        .map(fieldValue -> DynamicTest.dynamicTest(
                                fieldName + " = " + fieldValue,
                                () -> PackstreamConditions.requireNonNull(fieldName, fieldValue))));
    }

    @TestFactory
    Stream<DynamicTest> requireNonNullShouldAcceptNonNoneValues() {
        return Stream.of("foo", "bar", "baz")
                .flatMap(fieldName -> Stream.<AnyValue>of(Values.longValue(42), Values.stringValue("potato"))
                        .map(fieldValue -> DynamicTest.dynamicTest(
                                fieldName + " = " + fieldValue,
                                () -> PackstreamConditions.requireNonNullValue(fieldName, fieldValue))));
    }

    @TestFactory
    Stream<DynamicTest> requireNonNullShouldRejectNullValues() {
        return Stream.of("foo", "bar", "baz")
                .map(fieldName -> DynamicTest.dynamicTest(
                        fieldName + " = null",
                        () -> ErrorGqlStatusObjectAssertions.assertThatThrownBy(
                                        () -> PackstreamConditions.requireNonNull(fieldName, null))
                                .isInstanceOf(IllegalStructArgumentException.class)
                                .hasMessage(useNewMessage("08N06: General network protocol error.")
                                        .whenLegacyFallbackTo("Illegal value for field \"" + fieldName
                                                + "\": Expected value to be non-null"))
                                .hasNoCause()
                                .hasGqlStatus(GqlStatusInfoCodes.STATUS_08N06)
                                .hasStatusDescription(
                                        "error: connection exception - protocol error. General network protocol error.")
                                .gqlCause()
                                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N05)
                                .hasStatusDescription(
                                        "error: data exception - input failed validation. Invalid input 'null' for field '"
                                                + fieldName + "'.")
                                .gqlCause()
                                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22004)
                                .hasStatusDescription("error: data exception - null value not allowed")));
    }

    @TestFactory
    Stream<DynamicTest> requireNonNullShouldRejectNoneValues() {
        return Stream.of("foo", "bar", "baz")
                .map(fieldName -> DynamicTest.dynamicTest(
                        fieldName + " = null",
                        () -> ErrorGqlStatusObjectAssertions.assertThatThrownBy(
                                        () -> PackstreamConditions.requireNonNullValue(fieldName, Values.NO_VALUE))
                                .isInstanceOf(IllegalStructArgumentException.class)
                                .hasMessage(useNewMessage("08N06: General network protocol error.")
                                        .whenLegacyFallbackTo("Illegal value for field \"" + fieldName
                                                + "\": Expected value to be non-null"))
                                .hasNoCause()
                                .hasGqlStatus(GqlStatusInfoCodes.STATUS_08N06)
                                .hasStatusDescription(
                                        "error: connection exception - protocol error. General network protocol error.")
                                .gqlCause()
                                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N05)
                                .hasStatusDescription(
                                        "error: data exception - input failed validation. Invalid input 'null' for field '"
                                                + fieldName + "'.")
                                .gqlCause()
                                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22004)
                                .hasStatusDescription("error: data exception - null value not allowed")));
    }
}
