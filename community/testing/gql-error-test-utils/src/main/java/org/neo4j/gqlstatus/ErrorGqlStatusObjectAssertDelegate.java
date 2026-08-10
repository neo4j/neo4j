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

/**
 * Delegate for {@link ErrorGqlStatusObjectAssert} that allows for delegating all assertions to another assert object.
 * This is implemented as an interface rather than a class, because GqlExceptionLikeAssert already has a different
 * superclass.
 */
public interface ErrorGqlStatusObjectAssertDelegate<SELF extends ErrorGqlStatusObjectAssertDelegate<SELF>>
        extends ErrorGqlStatusObjectAssert<SELF> {
    /**
     * The assert object to delegate all assertions to.
     */
    ErrorGqlStatusObjectAssert<?> gqlStatusObject();

    @Override
    default ErrorGqlStatusObjectAssert<?> gqlCause() {
        return gqlStatusObject().gqlCause();
    }

    @Override
    default ErrorGqlStatusObjectAssert<?> gqlRootCauseOrSelf() {
        return gqlStatusObject().gqlRootCauseOrSelf();
    }

    @Override
    default SELF hasGqlStatus(GqlStatusInfoCodes expectedStatus) {
        gqlStatusObject().hasGqlStatus(expectedStatus);
        //noinspection unchecked
        return (SELF) this;
    }

    @Override
    default SELF hasStatusDescription(String expectedDescription) {
        gqlStatusObject().hasStatusDescription(expectedDescription);
        //noinspection unchecked
        return (SELF) this;
    }

    @Override
    default SELF hasStatusDescriptionContaining(String partialExpectedDescription) {
        gqlStatusObject().hasStatusDescriptionContaining(partialExpectedDescription);
        //noinspection unchecked
        return (SELF) this;
    }

    @Override
    default SELF hasStatusDescription(String expectedDescriptionTemplate, Object... args) {
        gqlStatusObject().hasStatusDescription(expectedDescriptionTemplate, args);
        //noinspection unchecked
        return (SELF) this;
    }
}
