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
package org.neo4j.bolt.protocol.common.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.gqlstatus.DiagnosticRecord;
import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.storageengine.api.txstate.validation.TransactionConflictException;

class ErrorTest {
    @Test
    void shouldAssignUnknownStatusToUnpredictedException() {
        // Given
        Throwable cause = new Throwable("This is not an error we know how to handle.");
        Error error = Error.from(cause);

        // Then
        assertThat(error.status()).isEqualTo(Status.General.UnknownError);
    }

    @Test
    void shouldConvertDeadlockException() {
        // When
        Error error = Error.from(new DeadlockDetectedException(null));

        // Then
        assertEquals(Status.Transaction.DeadlockDetected, error.status());
    }

    @Test
    void shouldSetStatusToDatabaseUnavailableOnDatabaseShutdownException() {
        // Given
        DatabaseShutdownException ex =
                DatabaseShutdownException.databaseUnavailable(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);

        // When
        Error error = Error.from(ex);

        // Then
        assertThat(error.status()).isEqualTo(Status.General.DatabaseUnavailable);
        assertThat(error.cause()).isEqualTo(ex);
        // GQL info
        assertInstanceOf(ErrorGqlStatusObject.class, error.wrappedThrowable());
        var gqlError = (ErrorGqlStatusObject) error.wrappedThrowable();
        assertEquals(gqlError.gqlStatus(), "08N09");
        assertEquals(
                gqlError.statusDescription(),
                "error: connection exception - database unavailable. The database `neo4j` is currently unavailable. Check the database status. Retry your request at a later time.");
    }

    @Nested
    class TestAsBoltMessage {

        @Test
        void doNotFailWithStackOverflowWhenCausesFormCircle() {
            // those two exceptions form the simplest loop
            var shutdownException =
                    DatabaseShutdownException.databaseUnavailable(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
            var transientException = new TransientTransactionFailureException(
                    shutdownException.gqlStatusObject(),
                    Status.Transaction.DeadlockDetected,
                    "test",
                    shutdownException);

            Error boltError = Error.from(transientException);
            assertThat(assertDoesNotThrow(boltError::asBoltMessage).metadata().description())
                    .contains("The database `neo4j` is currently unavailable.");
        }

        @Test
        void shouldAssignUnknownStatusToUnpredictedException() {
            // Given
            Throwable cause = new Throwable("This is not an error we know how to handle.");
            Error error = Error.from(cause);
            var metadata = error.asBoltMessage().metadata();

            // Then
            assertThat(metadata.status()).isEqualTo(Status.General.UnknownError);
            assertThat(metadata.message()).isEqualTo(cause.getMessage());
            assertThat(metadata.gqlStatus()).isEqualTo("50N00");
            assertThat(metadata.description())
                    .isEqualTo(
                            "error: general processing exception - internal error. Internal exception raised Throwable: This is not an error we know how to handle.");
            assertThat(metadata.diagnosticRecord())
                    .isEqualTo(DiagnosticRecord.from().build().asMap());
            assertThat(metadata.cause()).isNotNull();
            assertThat(metadata.cause().gqlStatus()).isEqualTo("50N09");
            assertThat(metadata.cause().description())
                    .isEqualTo(
                            "error: general processing exception - invalid server state transition. The server transitioned into a server state that is not valid in the current context: 'uncaught error'.");
        }

        @Test
        void shouldConvertDeadlockException() {
            // When
            var cause = new DeadlockDetectedException("Dead lock");
            Error error = Error.from(cause);
            var metadata = error.asBoltMessage().metadata();

            // Then
            assertThat(metadata.status()).isEqualTo(Status.Transaction.DeadlockDetected);
            assertThat(metadata.message()).isEqualTo(cause.getMessage());
            assertThat(metadata.gqlStatus()).isEqualTo(ErrorGqlStatusObject.DEFAULT_STATUS_CODE);
            assertThat(metadata.description()).isEqualTo(ErrorGqlStatusObject.DEFAULT_STATUS_DESCRIPTION);
            assertThat(metadata.diagnosticRecord())
                    .isEqualTo(DiagnosticRecord.from().build().asMap());
            assertThat(metadata.cause()).isNull();
        }

        @Test
        void shouldSetStatusToDatabaseUnavailableOnDatabaseShutdownException() {
            // Given
            DatabaseShutdownException ex = DatabaseShutdownException.databaseUnavailable("MyDb");

            // When
            Error error = Error.from(ex);

            var metadata = error.asBoltMessage().metadata();

            // Then
            assertThat(metadata.status()).isEqualTo(Status.General.DatabaseUnavailable);
            assertThat(metadata.message())
                    .isEqualTo(Status.General.DatabaseUnavailable.code().description());
            assertThat(metadata.cause()).isNull();
            assertThat(metadata.gqlStatus()).isEqualTo("08N09");
            assertThat(metadata.description())
                    .isEqualTo(
                            "error: connection exception - database unavailable. The database `MyDb` is currently unavailable. Check the database status. Retry your request at a later time.");
            assertThat(metadata.diagnosticRecord())
                    .isEqualTo(DiagnosticRecord.from()
                            .withClassification(ErrorClassification.TRANSIENT_ERROR)
                            .build()
                            .asMap());
        }

        @Test
        void shouldHandleGqlErrorWithoutCause() {
            // Given
            var ex = invalidArgumentException();

            // When
            var error = Error.from(ex);
            var metadata = error.asBoltMessage().metadata();

            // Then
            assertThat(metadata.status()).isEqualTo(ex.status());
            assertThat(metadata.message()).isEqualTo(ex.legacyMessage());
            assertThat(metadata.gqlStatus()).isEqualTo(ex.gqlStatus());
            assertThat(metadata.description()).isEqualTo(ex.statusDescription());
            assertThat(metadata.diagnosticRecord()).isEqualTo(ex.diagnosticRecord());
            assertThat(metadata.cause()).isNull();
        }

        @Test
        void shouldHandleExceptionCausedGqlErrorWithoutCause() {
            // Given
            var gqlEx = invalidArgumentException();
            var ex = new RuntimeException("Something is rotten", gqlEx);

            // When
            var error = Error.from(ex);
            var metadata = error.asBoltMessage().metadata();

            // Then
            assertThat(metadata.status()).isEqualTo(gqlEx.status());
            assertThat(metadata.message()).isEqualTo(gqlEx.legacyMessage());
            assertThat(metadata.gqlStatus()).isEqualTo(gqlEx.gqlStatus());
            assertThat(metadata.description()).isEqualTo(gqlEx.statusDescription());
            assertThat(metadata.diagnosticRecord()).isEqualTo(gqlEx.diagnosticRecord());
            assertThat(metadata.cause()).isNull();
        }

        @Test
        void shouldHandleGqlErrorWithCause() {
            // Given
            var cause = transactionConflictException();
            var ex = invalidArgumentException(cause);

            // When
            var error = Error.from(ex);
            var metadata = error.asBoltMessage().metadata();

            // Then
            assertThat(metadata.status()).isEqualTo(ex.status());
            assertThat(metadata.message()).isEqualTo(ex.legacyMessage());
            assertThat(metadata.gqlStatus()).isEqualTo(ex.gqlStatus());
            assertThat(metadata.description()).isEqualTo(ex.statusDescription());
            assertThat(metadata.diagnosticRecord()).isEqualTo(ex.diagnosticRecord());
            assertThat(metadata.cause()).isNotNull();

            var causeMetadata = metadata.cause();
            assertThat(causeMetadata.status()).isEqualTo(cause.status());
            assertThat(causeMetadata.message()).isEqualTo(cause.getMessage());
            assertThat(causeMetadata.gqlStatus()).isEqualTo(cause.gqlStatus());
            assertThat(causeMetadata.description()).isEqualTo(cause.statusDescription());
            assertThat(causeMetadata.diagnosticRecord()).isEqualTo(cause.diagnosticRecord());
            assertThat(causeMetadata.cause()).isNull();
        }

        @Test
        void shouldHandleExceptionCausedGqlErrorCause() {
            // Given
            var cause = transactionConflictException();
            var gqlEx = invalidArgumentException(cause);
            var ex = new RuntimeException("Something is rotten", gqlEx);

            // When
            var error = Error.from(ex);
            var metadata = error.asBoltMessage().metadata();

            // Then
            assertThat(metadata.status()).isEqualTo(gqlEx.status());
            assertThat(metadata.message()).isEqualTo(gqlEx.legacyMessage());
            assertThat(metadata.gqlStatus()).isEqualTo(gqlEx.gqlStatus());
            assertThat(metadata.description()).isEqualTo(gqlEx.statusDescription());
            assertThat(metadata.diagnosticRecord()).isEqualTo(gqlEx.diagnosticRecord());
            assertThat(metadata.cause()).isNotNull();

            var causeMetadata = metadata.cause();
            assertThat(causeMetadata.status()).isEqualTo(cause.status());
            assertThat(causeMetadata.message()).isEqualTo(cause.getMessage());
            assertThat(causeMetadata.gqlStatus()).isEqualTo(cause.gqlStatus());
            assertThat(causeMetadata.description()).isEqualTo(cause.statusDescription());
            assertThat(causeMetadata.diagnosticRecord()).isEqualTo(cause.diagnosticRecord());
            assertThat(causeMetadata.cause()).isNull();
        }

        static InvalidArgumentException invalidArgumentException() {
            return invalidArgumentException(null);
        }

        static InvalidArgumentException invalidArgumentException(Throwable exceptionCause) {
            var builder = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N85)
                    .withClassification(ErrorClassification.CLIENT_ERROR);

            if (exceptionCause instanceof ErrorGqlStatusObject cause) {
                builder = builder.withCause(cause);
            }

            var gql = builder.build();
            return new InvalidArgumentException(gql, "Can't specify both allowed and denied databases", exceptionCause);
        }

        static TransactionConflictException transactionConflictException() {
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N11)
                    .withClassification(ErrorClassification.TRANSIENT_ERROR)
                    .build();
            return new TransactionConflictException(
                    gql,
                    "Concurrent modification exception. Constraint to be removed already removed by another transaction.",
                    null);
        }
    }
}
