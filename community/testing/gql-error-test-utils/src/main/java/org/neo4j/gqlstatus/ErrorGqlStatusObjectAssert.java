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

import org.assertj.core.util.CanIgnoreReturnValue;

/**
 * Assertions for ErrorGqlStatusObject objects.
 */
public interface ErrorGqlStatusObjectAssert<SELF extends ErrorGqlStatusObjectAssert<SELF>> {
    /**
     * Returns a new assertion object that uses the gql cause of the current ErrorGqlStatusObjectAssert
     * as the actual object under test.
     */
    ErrorGqlStatusObjectAssert<?> gqlCause();

    /**
     * Returns a new assertion object that uses the gql root cause of the current ErrorGqlStatusObjectAssert
     * as the actual object under test, or itself if there is no cause.
     */
    ErrorGqlStatusObjectAssert<?> gqlRootCauseOrSelf();

    /**
     * Verifies that the actual GqlStatusObject has the given status.
     */
    @CanIgnoreReturnValue
    SELF hasGqlStatus(GqlStatusInfoCodes expectedStatus);

    /**
     * Verifies that the actual GqlStatusObject has the given description.
     */
    @CanIgnoreReturnValue
    SELF hasStatusDescription(String expectedDescription);

    /**
     * Verifies that the actual GqlStatusObject has a status descriptions which contains the given text.
     */
    @CanIgnoreReturnValue
    SELF hasStatusDescriptionContaining(String partialExpectedDescription);

    /**
     * Verifies that the actual GqlStatusObject has the given description, after being formatted using
     * the {@link String#format} method.
     */
    @CanIgnoreReturnValue
    SELF hasStatusDescription(String expectedDescriptionTemplate, Object... args);
}
