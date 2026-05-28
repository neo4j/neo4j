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
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public final class FailureMetadataAssertions extends AbstractGqlMetadataAssertionBuilder<FailureMetadataAssertions> {

    private static final String NEO4J_LEGACY_STATUS_KEY = "code";
    private static final String NEO4J_STATUS_KEY = "neo4j_code";

    private FailureMetadataAssertions() {}

    private FailureMetadataAssertions(Map<String, List<Assertion>> assertions, boolean lenient) {
        super(assertions, lenient);
    }

    public static FailureMetadataAssertions create() {
        return new FailureMetadataAssertions();
    }

    @Override
    protected FailureMetadataAssertions create(Map<String, List<Assertion>> assertions, boolean lenient) {
        return new FailureMetadataAssertions(assertions, lenient);
    }

    @Override
    public void evaluate(BoltWire wire, Map<String, Object> meta) {
        try {
            super.evaluate(wire, meta);
        } catch (AssertionError e) {
            var message = new StringBuilder("Failure message structure does not match expected structure:\n\n");
            prettyPrint(message, meta);

            var matchingKeys = this.matchingKeys(wire);
            var ignoredKeys = this.ignoredKeys(wire);

            if (!matchingKeys.isEmpty()) {
                message.append("\n\nMatched Assertions:");
                matchingKeys.forEach(key -> message.append("\n - ").append(key));
            }
            if (!ignoredKeys.isEmpty()) {
                message.append("\n\nIgnored Assertions:");
                ignoredKeys.forEach(key -> message.append("\n - ").append(key));
            }

            message.append("\n\nCheck the following cause section for additional information:");
            throw new AssertionError(message.toString(), e);
        }
    }

    @Override
    public FailureMetadataAssertions hasStatus(GqlStatusInfoCodes expected, GqlMessageParameters parameters) {
        return super.hasStatus(expected, parameters);
    }

    @Override
    public FailureMetadataAssertions hasStatus(GqlStatusInfoCodes expected) {
        return super.hasStatus(expected);
    }

    @Override
    protected FailureMetadataAssertions hasGqlMessage(String expected) {
        return this.registerAssertion(
                MESSAGE_KEY,
                Assertion.wrap(actual -> Assertions.assertThat(actual).isEqualTo(expected))
                        .withCondition(wire -> !wire.hasLegacyFailureMessages()));
    }

    /**
     * @deprecated Classic status codes will eventually be replaced with their GQL counterparts.
     */
    public FailureMetadataAssertions hasLegacyStatus(Status expected) {
        return this.registerAssertion(
                        NEO4J_STATUS_KEY,
                        Assertion.wrap(actual -> Assertions.assertThat(actual)
                                        .isEqualTo(expected.code().serialize()))
                                .withCondition(BoltWire::hasGQLStatus))
                .registerAssertion(
                        NEO4J_LEGACY_STATUS_KEY,
                        Assertion.wrap(actual -> Assertions.assertThat(actual)
                                        .isEqualTo(expected.code().serialize()))
                                .withCondition(wire -> !wire.hasGQLStatus()));
    }
}
