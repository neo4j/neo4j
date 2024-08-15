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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.mockStatic;

import org.junit.jupiter.api.Test;

public class HasGqlStatusInfoTest {

    @Test
    void testGetOldCauseMessage() {
        var gql1 = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N08)
                .withParam(GqlMessageParams.option1, "blabla")
                .withParam(GqlMessageParams.option2, "blabla")
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G10)
                        .build())
                .build();
        var gql2 = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_00000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_00001)
                        .build())
                .build();
        var someOtherGql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N09)
                .withParam(GqlMessageParams.db, "some db")
                .build();
        var oldMessage = "this is an old message";

        // This mocks such that the new gql message is always returned
        try (var errorMessageHolder = mockStatic(ErrorMessageHolder.class)) {
            errorMessageHolder
                    .when(() -> ErrorMessageHolder.getMessage(someOtherGql, oldMessage))
                    .thenReturn(someOtherGql.toString());
            errorMessageHolder
                    .when(() -> ErrorMessageHolder.getMessage(gql1, someOtherGql.toString()))
                    .thenReturn(gql1.toString());
            errorMessageHolder
                    .when(() -> ErrorMessageHolder.getMessage(gql2, gql1.toString()))
                    .thenReturn(gql2.toString());
            errorMessageHolder
                    .when(() -> ErrorMessageHolder.getMessage(gql2, gql2.toString()))
                    .thenReturn(gql2.toString());

            var exWithOut = new ExceptionWithoutCause(someOtherGql, oldMessage);
            var exceptionWithCause1 = new ExceptionWithCause(gql1, exWithOut);
            var exceptionWithCause2 = new ExceptionWithCause(gql2, exceptionWithCause1);
            var exceptionWithCause3 = new ExceptionWithCause(gql2, exceptionWithCause2);

            var oldEx1 = exceptionWithCause1.getOldMessage();
            var oldEx2 = exceptionWithCause2.getOldMessage();
            var oldEx3 = exceptionWithCause3.getOldMessage();

            var gqlEx1 = exceptionWithCause1.getMessage();
            var gqlEx2 = exceptionWithCause2.getMessage();
            var gqlEx3 = exceptionWithCause3.getMessage();

            assertEquals(oldEx1, oldMessage);
            assertEquals(oldEx2, oldMessage);
            assertEquals(oldEx3, oldMessage);

            assertNotEquals(oldEx1, gqlEx1);
            assertNotEquals(oldEx2, gqlEx2);
            assertNotEquals(oldEx3, gqlEx3);
            assertNotEquals(gqlEx1, gqlEx2);

            assertEquals(gqlEx3, gqlEx2); // equality since gql2 is in both exceptionWithCause2 and exceptionWithCause3
        }
    }

    static class ExceptionWithoutCause extends Exception implements HasGqlStatusInfo {
        private final ErrorGqlStatusObject gqlStatusObject;
        private final String oldMessage;

        ExceptionWithoutCause(ErrorGqlStatusObject gqlStatusObject, String oldMessage) {
            super(ErrorMessageHolder.getMessage(gqlStatusObject, oldMessage));
            this.gqlStatusObject = gqlStatusObject;
            this.oldMessage = oldMessage;
        }

        @Override
        public String getOldMessage() {
            return oldMessage;
        }

        @Override
        public ErrorGqlStatusObject gqlStatusObject() {
            return gqlStatusObject;
        }
    }

    static class ExceptionWithCause extends Exception implements HasGqlStatusInfo {
        private final ErrorGqlStatusObject gqlStatusObject;
        private final String oldMessage;

        public ExceptionWithCause(ErrorGqlStatusObject gqlStatusObject, Throwable cause) {
            super(ErrorMessageHolder.getMessage(gqlStatusObject, cause.getMessage()));
            this.gqlStatusObject = gqlStatusObject;
            // This logic is what we test
            this.oldMessage = HasGqlStatusInfo.getOldCauseMessage(cause);
        }

        @Override
        public String getOldMessage() {
            return oldMessage;
        }

        @Override
        public ErrorGqlStatusObject gqlStatusObject() {
            return gqlStatusObject;
        }
    }
}
