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
package org.neo4j.internal.kernel.api.security;

import org.neo4j.common.Subject;

public interface AuthSubject {
    // TODO: Refine this API into something more polished
    AuthenticationResult getAuthenticationResult();

    /**
     * @param username a username
     * @return true if the provided username is the same as the executing username of this subject
     */
    boolean hasUsername(String username);

    /**
     * Get the name of the user associated with the current transaction.
     * This might be the same as the authenticated user or a different when using impersonation
     * @return the username
     */
    String executingUser();

    /**
     * Get the name of the authenticated user
     * @return the username
     */
    default String authenticatedUser() {
        return executingUser();
    }

    default boolean nativelyAuthenticated() {
        return true;
    }

    default Subject userSubject() {
        if (AuthSubject.AUTH_DISABLED == this) {
            return Subject.AUTH_DISABLED;
        } else if (AuthSubject.ANONYMOUS == this) {
            return Subject.ANONYMOUS;
        } else {
            return new Subject(this.executingUser());
        }
    }

    /**
     * Implementation to use when authentication has not yet been performed. Allows nothing.
     */
    AuthSubject ANONYMOUS = new AuthSubject() {

        @Override
        public AuthenticationResult getAuthenticationResult() {
            return AuthenticationResult.FAILURE;
        }

        @Override
        public boolean hasUsername(String username) {
            return false;
        }

        @Override
        public String executingUser() {
            return ""; // Should never clash with a valid username
        }
    };

    /**
     * Implementation to use when authentication is disabled. Allows everything.
     */
    AuthSubject AUTH_DISABLED = new AuthSubject() {
        @Override
        public String executingUser() {
            return ""; // Should never clash with a valid username
        }

        @Override
        public AuthenticationResult getAuthenticationResult() {
            return AuthenticationResult.SUCCESS;
        }

        @Override
        public boolean hasUsername(String username) {
            return false;
        }
    };
}
