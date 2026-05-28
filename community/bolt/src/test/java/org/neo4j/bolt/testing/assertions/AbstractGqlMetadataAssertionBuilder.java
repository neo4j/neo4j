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
import org.assertj.core.api.InstanceOfAssertFactories;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.gqlstatus.GqlStatus;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;

public abstract class AbstractGqlMetadataAssertionBuilder<SELF extends AbstractGqlMetadataAssertionBuilder<SELF>>
        extends AbstractMetadataAssertionBuilder<SELF> {

    protected static final String CAUSE_KEY = "cause";
    protected static final String DESCRIPTION_KEY = "description";
    protected static final String DIAGNOSTIC_RECORD_KEY = "diagnostic_record";
    protected static final String GQL_STATUS_KEY = "gql_status";
    protected static final String MESSAGE_KEY = "message";

    protected AbstractGqlMetadataAssertionBuilder() {}

    protected AbstractGqlMetadataAssertionBuilder(Map<String, List<Assertion>> assertions, boolean lenient) {
        super(assertions, lenient);
    }

    protected SELF hasMessage(String expected) {
        return this.registerAssertion(
                MESSAGE_KEY, actual -> Assertions.assertThat(actual).isEqualTo(expected));
    }

    /**
     * @deprecated Classic message strings will eventually be replaced with their GQL counterparts.
     */
    @Deprecated
    public SELF hasLegacyMessage(String message) {
        return this.registerAssertion(
                MESSAGE_KEY,
                Assertion.wrap(actual -> Assertions.assertThat(actual).isEqualTo(message))
                        .withCondition(BoltWire::hasLegacyFailureMessages));
    }

    /**
     * @deprecated Classic message strings will eventually be replaced with their GQL counterparts.
     */
    @Deprecated
    public SELF hasLegacyMessageFuzzy(String message) {
        return this.registerAssertion(
                MESSAGE_KEY,
                Assertion.wrap(actual ->
                                Assertions.assertThat(actual).asString().contains(message))
                        .withCondition(BoltWire::hasLegacyFailureMessages));
    }

    protected SELF hasGqlMessage(String expected) {
        return this.registerAssertion(
                MESSAGE_KEY,
                Assertion.wrap(actual -> Assertions.assertThat(actual).isEqualTo(expected))
                        .withCondition(wire -> !wire.hasLegacyFailureMessages()));
    }

    public SELF hasCause(FailureCauseAssertions assertions) {
        return this.registerAssertion(
                CAUSE_KEY,
                Assertion.wrap((wire, actual) -> Assertions.assertThat(actual)
                                .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                                .satisfies(meta -> assertions.evaluate(wire, meta)))
                        .withCondition(BoltWire::hasGQLStatus));
    }

    protected SELF hasStatus(GqlStatus expected) {
        return this.registerAssertion(
                GQL_STATUS_KEY,
                Assertion.wrap(actual ->
                                Assertions.assertThat(actual).asString().isEqualTo(expected.gqlStatusString()))
                        .withCondition(BoltWire::hasGQLStatus));
    }

    @Deprecated
    public SELF hasNewStatus(GqlStatusInfoCodes expected, GqlMessageParameters parameters) {
        return this.hasStatus(expected, parameters).hasMessage(this.generateGqlMessage(expected, parameters));
    }

    @Deprecated
    public SELF hasNewStatus(GqlStatusInfoCodes expected) {
        return this.hasNewStatus(expected, GqlMessageParameters.create());
    }

    protected String generateGqlMessage(GqlStatusInfoCodes expected, GqlMessageParameters parameters) {
        var message = expected.getStatusString();
        var template = expected.getTemplate();
        if (!template.isEmpty()) {
            // this isn't ideal since we no longer explicitly define the message resulting from a
            // given failure but should be covered by unit tests - readability is preferred here for
            // now
            message += ": " + expected.getMessage(parameters.toArray());
        }
        return message;
    }

    public SELF hasStatus(GqlStatusInfoCodes expected, GqlMessageParameters parameters) {
        return this.hasStatus(expected.getGqlStatus()).hasGqlMessage(this.generateGqlMessage(expected, parameters));
    }

    public SELF hasStatus(GqlStatusInfoCodes expected) {
        return this.hasStatus(expected, GqlMessageParameters.create());
    }

    public SELF hasStatus(GqlStatus status, String message) {
        return this.registerAssertion(
                        GQL_STATUS_KEY,
                        Assertion.wrap(actual -> Assertions.assertThat(actual).isEqualTo(status.gqlStatusString()))
                                .withCondition(BoltWire::hasGQLStatus))
                .registerAssertion(
                        MESSAGE_KEY,
                        Assertion.wrap(actual -> Assertions.assertThat(actual)
                                        .isEqualTo(status.gqlStatusString() + ": " + message))
                                .withCondition(wire -> !wire.hasLegacyFailureMessages()));
    }

    public SELF hasDescription(String expected) {
        return this.registerAssertion(
                DESCRIPTION_KEY,
                Assertion.wrap(actual ->
                                Assertions.assertThat(actual).asString().isEqualTo(expected))
                        .withCondition(BoltWire::hasGQLStatus));
    }

    public SELF hasDescriptionFuzzy(String expected) {
        return this.registerAssertion(
                DESCRIPTION_KEY,
                Assertion.wrap(actual ->
                                Assertions.assertThat(actual).asString().contains(expected))
                        .withCondition(BoltWire::hasGQLStatus));
    }

    public SELF hasDiagnosticRecord(DiagnosticRecordAssertions assertions) {
        return this.registerAssertion(
                DIAGNOSTIC_RECORD_KEY,
                Assertion.wrap((wire, actual) -> Assertions.assertThat(actual)
                                .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                                .satisfies(meta -> assertions.evaluate(wire, meta)))
                        .withCondition(BoltWire::hasGQLStatus));
    }
}
