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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Map;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.jupiter.api.Test;

class ErrorGqlStatusObjectImplementationTest {
    @Test
    void shouldHandleErrorWithDuplicatedParameter() {
        var errorBuilder = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N02);
        errorBuilder.withParam(GqlParams.StringParam.db, "my_db"); // this parameter occurs twice in the message
        errorBuilder.withParam(GqlParams.StringParam.cfgSetting, "my_setting");
        var error = errorBuilder.build();

        assertThat(error.statusDescription())
                .isEqualTo(
                        "error: connection exception - unable to route to database. Unable to connect to database `my_db`. Server-side routing is disabled. Either connect to `my_db` directly, or enable server-side routing by setting 'my_setting=true'.");
    }

    @Test
    void shouldNotFailOnErrorWithTooFewParameters() {
        var errorBuilder = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N02);
        var error = errorBuilder.build();

        assertThat(error.statusDescription())
                .isEqualTo(
                        "error: procedure exception - procedure execution client error. Execution of the procedure $proc() failed due to a client error.");
    }

    @Test
    void shouldNotFailOnErrorWithTooManyParameters() {
        var errorBuilder = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N02);
        errorBuilder.withParam(GqlParams.StringParam.propKey, "bar");
        errorBuilder.withParam(GqlParams.StringParam.proc, "my_proc");
        var error = errorBuilder.build();

        assertThat(error.statusDescription())
                .isEqualTo(
                        "error: procedure exception - procedure execution client error. Execution of the procedure my_proc() failed due to a client error.");
    }

    @Test
    void shouldNotFailOnErrorWithWrongParameter() {
        var errorBuilder = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N02);
        errorBuilder.withParam(GqlParams.StringParam.propKey, "bar");
        var error = errorBuilder.build();

        assertThat(error.statusDescription())
                .isEqualTo(
                        "error: procedure exception - procedure execution client error. Execution of the procedure $proc() failed due to a client error.");
    }

    @Test
    void shouldBeSerializedAndDeserialized() {
        var errorBuilder = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N02);
        errorBuilder.withParam(GqlParams.StringParam.db, "my_db"); // this parameter occurs twice in the message
        errorBuilder.withParam(GqlParams.StringParam.cfgSetting, "my_setting");
        var error = errorBuilder.build();

        byte[] data = SerializationUtils.serialize((ErrorGqlStatusObjectImplementation) error.gqlStatusObject());

        ErrorGqlStatusObjectImplementation deserialized = SerializationUtils.deserialize(data);

        assertThat(deserialized).isEqualTo(error);
    }

    @Test
    void positionAssertShouldWorkForValidPositions() {
        var errorBuilder = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N02);

        // Default/test positions with line -1 or 0
        assertThatCode(() -> errorBuilder.atPosition(0, 0, 0)).doesNotThrowAnyException();
        assertThatCode(() -> errorBuilder.atPosition(1, 0, 1)).doesNotThrowAnyException();
        assertThatCode(() -> errorBuilder.atPosition(42, 0, 37)).doesNotThrowAnyException();
        assertThatCode(() -> errorBuilder.atPosition(-1, -1, -1)).doesNotThrowAnyException();

        // On line 1, column is always offset + 1
        assertThatCode(() -> errorBuilder.atPosition(0, 1, 1)).doesNotThrowAnyException();
        assertThatCode(() -> errorBuilder.atPosition(10, 1, 11)).doesNotThrowAnyException();

        // On line > 1, offset must be at least as big as column
        assertThatCode(() -> errorBuilder.atPosition(7, 2, 7)).doesNotThrowAnyException();
        assertThatCode(() -> errorBuilder.atPosition(47, 2, 37)).doesNotThrowAnyException();
        assertThatCode(() -> errorBuilder.atPosition(23, 5, 1)).doesNotThrowAnyException();
    }

    @Test
    void positionAssertShouldFailForInvalidPositions() {
        var errorBuilder = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N02);

        // On line 1, column is always offset + 1
        assertThatExceptionOfType(AssertionError.class).isThrownBy(() -> errorBuilder.atPosition(2, 1, 2));
        assertThatExceptionOfType(AssertionError.class).isThrownBy(() -> errorBuilder.atPosition(2, 1, 1));

        // On line > 1, offset must be at least as big as column
        assertThatExceptionOfType(AssertionError.class).isThrownBy(() -> errorBuilder.atPosition(6, 2, 7));
        assertThatExceptionOfType(AssertionError.class).isThrownBy(() -> errorBuilder.atPosition(0, 5, 1));
    }

    @Test
    void shouldPropagatePositionToCauses() {
        ErrorGqlStatusObject error = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(2, 1, 3)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N87)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NA0)
                                .build())
                        .build())
                .build();

        var expectedPosition = Map.of("offset", 2, "line", 1, "column", 3);
        var cause = (ErrorGqlStatusObjectImplementation) error.cause().get();
        var innerCause = (ErrorGqlStatusObjectImplementation) cause.cause().get();

        assertThat(((ErrorGqlStatusObjectImplementation) error).diagnosticRecord.getPositionMap())
                .isEqualTo(expectedPosition);
        assertThat(cause.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition);
        assertThat(innerCause.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition);
    }

    @Test
    void shouldPropagatePositionToTop() {
        ErrorGqlStatusObject error = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N87)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NA0)
                                .atPosition(42, 3, 17)
                                .build())
                        .build())
                .build();

        var expectedPosition = Map.of("offset", 42, "line", 3, "column", 17);
        var cause = (ErrorGqlStatusObjectImplementation) error.cause().get();
        var innerCause = (ErrorGqlStatusObjectImplementation) cause.cause().get();

        assertThat(((ErrorGqlStatusObjectImplementation) error).diagnosticRecord.getPositionMap())
                .isEqualTo(expectedPosition);
        assertThat(cause.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition);
        assertThat(innerCause.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition);
    }

    @Test
    void shouldPropagateWithDifferentPositions() {
        ErrorGqlStatusObject error = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N87)
                        .atPosition(2, 1, 3)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NA0)
                                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NA1)
                                        .atPosition(42, 3, 17)
                                        .withCause(
                                                ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NA2)
                                                        .build())
                                        .build())
                                .build())
                        .build())
                .build();

        var expectedPosition1 = Map.of("offset", 2, "line", 1, "column", 3);
        var expectedPosition2 = Map.of("offset", 42, "line", 3, "column", 17);

        var cause1 = (ErrorGqlStatusObjectImplementation) error.cause().get();
        var cause2 = (ErrorGqlStatusObjectImplementation) cause1.cause().get();
        var cause3 = (ErrorGqlStatusObjectImplementation) cause2.cause().get();
        var cause4 = (ErrorGqlStatusObjectImplementation) cause3.cause().get();

        assertThat(((ErrorGqlStatusObjectImplementation) error).diagnosticRecord.getPositionMap())
                .isEqualTo(expectedPosition1);
        assertThat(cause1.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition1);
        assertThat(cause2.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition2);
        assertThat(cause3.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition2);
        assertThat(cause4.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition2);
    }

    @Test
    void shouldPropagatePositionToCausesOnSetCause() {
        ErrorGqlStatusObject error = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(2, 1, 3)
                .build();

        ((ErrorGqlStatusObjectImplementation) error)
                .setCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N87)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NA0)
                                .build())
                        .build());

        var expectedPosition = Map.of("offset", 2, "line", 1, "column", 3);
        var cause = (ErrorGqlStatusObjectImplementation) error.cause().get();
        var innerCause = (ErrorGqlStatusObjectImplementation) cause.cause().get();

        assertThat(((ErrorGqlStatusObjectImplementation) error).diagnosticRecord.getPositionMap())
                .isEqualTo(expectedPosition);
        assertThat(cause.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition);
        assertThat(innerCause.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition);
    }

    @Test
    void shouldPropagatePositionFromCausesOnSetCause() {
        ErrorGqlStatusObject error = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .build();

        ((ErrorGqlStatusObjectImplementation) error)
                .setCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N87)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NA0)
                                .atPosition(2, 1, 3)
                                .build())
                        .build());

        var expectedPosition = Map.of("offset", 2, "line", 1, "column", 3);
        var cause = (ErrorGqlStatusObjectImplementation) error.cause().get();
        var innerCause = (ErrorGqlStatusObjectImplementation) cause.cause().get();

        assertThat(((ErrorGqlStatusObjectImplementation) error).diagnosticRecord.getPositionMap())
                .isEqualTo(expectedPosition);
        assertThat(cause.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition);
        assertThat(innerCause.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition);
    }

    @Test
    void shouldPropagatePositionToCausesOnInsertCause() {
        ErrorGqlStatusObject error = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(2, 1, 3)
                .build();

        ((ErrorGqlStatusObjectImplementation) error)
                .insertCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N87)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NA0)
                                .build())
                        .buildImpl());

        var expectedPosition = Map.of("offset", 2, "line", 1, "column", 3);
        var cause = (ErrorGqlStatusObjectImplementation) error.cause().get();
        var innerCause = (ErrorGqlStatusObjectImplementation) cause.cause().get();

        assertThat(((ErrorGqlStatusObjectImplementation) error).diagnosticRecord.getPositionMap())
                .isEqualTo(expectedPosition);
        assertThat(cause.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition);
        assertThat(innerCause.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition);
    }

    @Test
    void shouldPropagatePositionFromCausesOnInsertCause() {
        ErrorGqlStatusObject error = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .build();

        ((ErrorGqlStatusObjectImplementation) error)
                .insertCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N87)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NA0)
                                .atPosition(2, 1, 3)
                                .build())
                        .buildImpl());

        var expectedPosition = Map.of("offset", 2, "line", 1, "column", 3);
        var cause = (ErrorGqlStatusObjectImplementation) error.cause().get();
        var innerCause = (ErrorGqlStatusObjectImplementation) cause.cause().get();

        assertThat(((ErrorGqlStatusObjectImplementation) error).diagnosticRecord.getPositionMap())
                .isEqualTo(expectedPosition);
        assertThat(cause.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition);
        assertThat(innerCause.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition);
    }

    @Test
    void positionPropagationShouldNotCauseStackOverflow() {
        final var ex1 = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .build();
        final var ex2 = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N87)
                .build();
        final var ex3 = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NA0)
                .build();
        ((ErrorGqlStatusObjectImplementation) ex1).setCause(ex2);
        ((ErrorGqlStatusObjectImplementation) ex2).setCause(ex3);
        ((ErrorGqlStatusObjectImplementation) ex3).setCause(ex1);

        ErrorGqlStatusObject error = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB8)
                .atPosition(2, 1, 3)
                .withCause(ex1)
                .build();

        var expectedPosition = Map.of("offset", 2, "line", 1, "column", 3);
        var cause1 = (ErrorGqlStatusObjectImplementation) error.cause().get();
        var cause2 = (ErrorGqlStatusObjectImplementation) cause1.cause().get();
        var cause3 = (ErrorGqlStatusObjectImplementation) cause2.cause().get();

        assertThat(((ErrorGqlStatusObjectImplementation) error).diagnosticRecord.getPositionMap())
                .isEqualTo(expectedPosition);
        assertThat(cause1.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition);
        assertThat(cause2.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition);
        assertThat(cause3.diagnosticRecord.getPositionMap()).isEqualTo(expectedPosition);
        assertThat(cause3.cause()).isEmpty();
    }
}
