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
package org.neo4j.gqlstatus;

import static java.lang.String.format;

import java.util.Optional;
import org.assertj.core.api.AbstractAssert;

public class ErrorGqlStatusObjectAssertImplementation
        extends AbstractAssert<ErrorGqlStatusObjectAssertImplementation, ErrorGqlStatusObject>
        implements ErrorGqlStatusObjectAssert<ErrorGqlStatusObjectAssertImplementation> {

    public ErrorGqlStatusObjectAssertImplementation(ErrorGqlStatusObject errorGqlStatusObject) {
        super(errorGqlStatusObject, ErrorGqlStatusObjectAssertImplementation.class);
    }

    @Override
    public ErrorGqlStatusObjectAssertImplementation gqlCause() {
        isNotNull();
        return new ErrorGqlStatusObjectAssertImplementation(causeOf(actual)
                .orElseThrow(() -> failure("Expected gql cause to be present, but was not. %s", actual)));
    }

    @Override
    public ErrorGqlStatusObjectAssertImplementation gqlRootCauseOrSelf() {
        isNotNull();
        var gql = actual;
        for (var cause = causeOf(gql); cause.isPresent(); cause = causeOf(gql)) {
            gql = cause.get();
        }
        return gql == actual ? this : new ErrorGqlStatusObjectAssertImplementation(gql);
    }

    private Optional<ErrorGqlStatusObject> causeOf(ErrorGqlStatusObject gqlStatusObject) {
        var cause = gqlStatusObject.cause();
        objects.assertNotNull(info, cause);
        return cause;
    }

    @Override
    public ErrorGqlStatusObjectAssertImplementation hasGqlStatus(GqlStatusInfoCodes expectedStatus) {
        isNotNull();
        if (!actual.gqlStatus().equals(expectedStatus.getStatusString())) {
            failWithMessage(
                    "Expected GqlStatusObject to have status <%s> but was <%s>",
                    expectedStatus.getStatusString(), actual.gqlStatus());
        }
        return this;
    }

    @Override
    public ErrorGqlStatusObjectAssertImplementation hasStatusDescription(String expectedDescription) {
        isNotNull();
        if (!actual.statusDescription().equals(expectedDescription)) {
            failWithMessage(
                    "Expected GqlStatusObject to have statusDescription <%s> but was <%s>",
                    expectedDescription, actual.statusDescription());
        }
        return this;
    }

    @Override
    public ErrorGqlStatusObjectAssertImplementation hasStatusDescriptionContaining(String partialExpectedDescription) {
        isNotNull();
        if (!actual.statusDescription().contains(partialExpectedDescription)) {
            failWithMessage(
                    "Expected GqlStatusObject to have statusDescription containing <%s> but was <%s>",
                    partialExpectedDescription, actual.statusDescription());
        }
        return this;
    }

    @Override
    public ErrorGqlStatusObjectAssertImplementation hasStatusDescription(
            String expectedDescriptionTemplate, Object... args) {
        isNotNull();
        var expectedDescription = format(expectedDescriptionTemplate, args);
        if (!actual.statusDescription().equals(expectedDescription)) {
            failWithMessage(
                    "Expected GqlStatusObject to have statusDescription <%s> but was <%s>",
                    expectedDescription, actual.statusDescription());
        }
        return this;
    }
}
