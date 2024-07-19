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
package org.neo4j.server.security.auth;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.FAILURE;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.SUCCESS;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.TOO_MANY_ATTEMPTS;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;

class ShiroAuthenticationInfoTest {
    private final Neo4jPrincipal principal = new Neo4jPrincipal("user");
    private final ShiroAuthenticationInfo successInfo = new ShiroAuthenticationInfo(principal, "realm", SUCCESS);
    private final ShiroAuthenticationInfo failureInfo = new ShiroAuthenticationInfo(principal, "realm", FAILURE);
    private final ShiroAuthenticationInfo tooManyAttemptsInfo =
            new ShiroAuthenticationInfo(principal, "realm", TOO_MANY_ATTEMPTS);
    private final ShiroAuthenticationInfo pwChangeRequiredInfo =
            new ShiroAuthenticationInfo(principal, "realm", PASSWORD_CHANGE_REQUIRED);

    // These tests are here to remind you that you need to update the ShiroAuthenticationInfo.mergeMatrix[][]
    // whenever you add/remove/move values in the AuthenticationResult enum

    @Test
    void shouldChangeMergeMatrixIfAuthenticationResultEnumChanges() {
        // These are the assumptions made for ShiroAuthenticationInfo.mergeMatrix[][]
        // which have to stay in sync with the enum
        assertEquals(AuthenticationResult.SUCCESS.ordinal(), 0);
        assertEquals(AuthenticationResult.FAILURE.ordinal(), 1);
        assertEquals(AuthenticationResult.TOO_MANY_ATTEMPTS.ordinal(), 2);
        assertEquals(AuthenticationResult.PASSWORD_CHANGE_REQUIRED.ordinal(), 3);
        assertEquals(4, AuthenticationResult.values().length);
    }

    @Test
    void shouldMergeTwoSuccessToSameValue() {
        ShiroAuthenticationInfo info = new ShiroAuthenticationInfo(principal, "realm", SUCCESS);
        info.merge(successInfo);

        assertEquals(SUCCESS, info.getAuthenticationResult());
    }

    @Test
    void shouldMergeTwoFailureToSameValue() {
        ShiroAuthenticationInfo info = new ShiroAuthenticationInfo(principal, "realm", FAILURE);
        info.merge(failureInfo);

        assertEquals(FAILURE, info.getAuthenticationResult());
    }

    @Test
    void shouldMergeTwoTooManyAttemptsToSameValue() {
        ShiroAuthenticationInfo info = new ShiroAuthenticationInfo(principal, "realm", TOO_MANY_ATTEMPTS);
        info.merge(tooManyAttemptsInfo);

        assertEquals(TOO_MANY_ATTEMPTS, info.getAuthenticationResult());
    }

    @Test
    void shouldMergeTwoPasswordChangeRequiredToSameValue() {
        ShiroAuthenticationInfo info = new ShiroAuthenticationInfo(principal, "realm", PASSWORD_CHANGE_REQUIRED);
        info.merge(pwChangeRequiredInfo);

        assertEquals(PASSWORD_CHANGE_REQUIRED, info.getAuthenticationResult());
    }

    @Test
    void shouldMergeFailureWithSuccessToNewValue() {
        ShiroAuthenticationInfo info = new ShiroAuthenticationInfo(principal, "realm", FAILURE);
        info.merge(successInfo);

        assertEquals(SUCCESS, info.getAuthenticationResult());
    }

    @Test
    void shouldMergeFailureWithTooManyAttemptsToNewValue() {
        ShiroAuthenticationInfo info = new ShiroAuthenticationInfo(principal, "realm", FAILURE);
        info.merge(tooManyAttemptsInfo);

        assertEquals(TOO_MANY_ATTEMPTS, info.getAuthenticationResult());
    }

    @Test
    void shouldMergeFailureWithPasswordChangeRequiredToNewValue() {
        ShiroAuthenticationInfo info = new ShiroAuthenticationInfo(principal, "realm", FAILURE);
        info.merge(pwChangeRequiredInfo);

        assertEquals(PASSWORD_CHANGE_REQUIRED, info.getAuthenticationResult());
    }

    @Test
    void shouldMergeToNewValue() {
        ShiroAuthenticationInfo info = new ShiroAuthenticationInfo(principal, "realm", FAILURE);
        info.merge(pwChangeRequiredInfo);

        assertEquals(PASSWORD_CHANGE_REQUIRED, info.getAuthenticationResult());
    }

    @Test
    void shouldMergeValidityChecksIntoList() {
        var validityCheck1 = mock(ValidityCheck.class);
        var validityCheck2 = mock(ValidityCheck.class);

        ShiroAuthenticationInfo info = new ShiroAuthenticationInfo(principal, "realm", SUCCESS);
        ShiroAuthenticationInfo info2 = new ShiroAuthenticationInfo(principal, "realm", SUCCESS) {
            {
                validityChecks.add(validityCheck1);
            }
        };
        ShiroAuthenticationInfo info3 = new ShiroAuthenticationInfo(principal, "realm", SUCCESS) {
            {
                validityChecks.add(validityCheck2);
            }
        };

        info.merge(info2);
        info.merge(info3);

        assertThat(info.validityChecks, containsInAnyOrder(validityCheck1, validityCheck2));
    }
}
