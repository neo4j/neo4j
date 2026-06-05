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

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

abstract class GqlExceptionTestBase {

    abstract <T extends Exception & ErrorGqlStatusObject> T testException(
            ErrorGqlStatusObject innerObject, String message);

    abstract <T extends Exception & ErrorGqlStatusObject> T testException(
            ErrorGqlStatusObject innerObject, String message, Throwable cause);

    @Test
    void shouldNotConvertJavaExceptionToCause() {
        ErrorGqlStatusObject gqlObject = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N11)
                .build();
        Throwable cause = new RuntimeException();
        ErrorGqlStatusObject errorWithJavaExceptionCause = testException(gqlObject, "message", cause);
        assertThat(errorWithJavaExceptionCause.cause()).isEmpty();
    }

    @Test
    void shouldConvertGqlExceptionToCause() {
        ErrorGqlStatusObject gqlObject = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N11)
                .build();
        ErrorGqlStatusObject causeGqlObject = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N08)
                .withParam(GqlParams.StringParam.option1, "myOption")
                .withParam(GqlParams.StringParam.option2, "yourOption")
                .build();
        Throwable cause = testException(causeGqlObject, "inner message");
        ErrorGqlStatusObject errorWithGqlExceptionCause = testException(gqlObject, "message", cause);
        assertThat(errorWithGqlExceptionCause.cause()).isPresent();
        ErrorGqlStatusObject firstCause = errorWithGqlExceptionCause.cause().get();
        assertThat(firstCause.gqlStatus()).isEqualTo(GqlStatusInfoCodes.STATUS_22N08.getStatusString());
        assertThat(firstCause.statusDescription()).contains("cannot combine 'myOption' with 'yourOption'");
        assertThat(firstCause.cause()).isNotPresent();
    }

    @Test
    void shouldConvertNotYetPortedGqlExceptionToStandardCause() {
        ErrorGqlStatusObject gqlObject = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N11)
                .build();
        Throwable cause = testException(null, "inner message");
        ErrorGqlStatusObject errorWithGqlExceptionCause = testException(gqlObject, "message", cause);
        assertThat(errorWithGqlExceptionCause.cause()).isPresent();
        ErrorGqlStatusObject firstCause = errorWithGqlExceptionCause.cause().get();
        assertThat(firstCause.gqlStatus()).isEqualTo(GqlStatusInfoCodes.STATUS_50N42.getStatusString());
        assertThat(firstCause.cause()).isNotPresent();
    }

    @Test
    void shouldAppendGqlExceptionToExistingGqlCause() {
        ErrorGqlStatusObject existingCauseGqlObject = ErrorGqlStatusObjectImplementation.from(
                        GqlStatusInfoCodes.STATUS_22N09)
                .withParam(GqlParams.StringParam.option, "option")
                .atPosition(7, 2, 3)
                .build();

        ErrorGqlStatusObject gqlObject = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N11)
                .withCause(existingCauseGqlObject)
                .build();

        ErrorGqlStatusObject exceptionCauseGqlObject = ErrorGqlStatusObjectImplementation.from(
                        GqlStatusInfoCodes.STATUS_22N08)
                .withParam(GqlParams.StringParam.option1, "myOption")
                .withParam(GqlParams.StringParam.option2, "yourOption")
                .build();

        Throwable cause = testException(exceptionCauseGqlObject, "inner message");
        ErrorGqlStatusObject errorWithGqlExceptionCause = testException(gqlObject, "message", cause);

        assertThat(errorWithGqlExceptionCause.cause()).isPresent();
        ErrorGqlStatusObject firstCause = errorWithGqlExceptionCause.cause().get();
        assertThat(firstCause.gqlStatus()).isEqualTo(GqlStatusInfoCodes.STATUS_22N09.getStatusString());
        assertThat(firstCause.statusDescription()).contains("conflicting values for 'option'.");
        Object position = firstCause.diagnosticRecord().get("_position");
        assertThat(position).isEqualTo(Map.of("offset", 7, "line", 2, "column", 3));

        assertThat(firstCause.cause()).isPresent();
        ErrorGqlStatusObject secondCause = firstCause.cause().get();
        assertThat(secondCause.gqlStatus()).isEqualTo(GqlStatusInfoCodes.STATUS_22N08.getStatusString());
        assertThat(secondCause.statusDescription()).contains("cannot combine 'myOption' with 'yourOption'");
        assertThat(secondCause.cause()).isNotPresent();
    }

    @Test
    void shouldNotAppendGqlExceptionToExistingGqlCausesIfAlreadyPresent() {
        ErrorGqlStatusObject causeGqlObject = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N09)
                .withParam(GqlParams.StringParam.option, "option")
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N08)
                        .withParam(GqlParams.StringParam.option1, "myOption")
                        .withParam(GqlParams.StringParam.option2, "yourOption")
                        .build())
                .build();

        var cause = testException(causeGqlObject, "inner message");

        ErrorGqlStatusObject gqlObject = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N11)
                .withCause(cause)
                .build();

        ErrorGqlStatusObject errorWithGqlExceptionCause = testException(gqlObject, "message", cause);

        assertThat(errorWithGqlExceptionCause.cause()).isPresent();
        ErrorGqlStatusObject firstCause = errorWithGqlExceptionCause.cause().get();
        assertThat(firstCause.gqlStatus()).isEqualTo(GqlStatusInfoCodes.STATUS_22N09.getStatusString());
        assertThat(firstCause.statusDescription()).contains("conflicting values for 'option'.");

        assertThat(firstCause.cause()).isPresent();
        ErrorGqlStatusObject secondCause = firstCause.cause().get();
        assertThat(secondCause.gqlStatus()).isEqualTo(GqlStatusInfoCodes.STATUS_22N08.getStatusString());
        assertThat(secondCause.statusDescription()).contains("cannot combine 'myOption' with 'yourOption'");

        assertThat(secondCause.cause()).isEmpty();
    }

    @Test
    void shouldAppendGqlExceptionToExistingGqlCausesIfAlmostSameCauseAlreadyPresent() {
        ErrorGqlStatusObject causeGqlObject = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N09)
                .withParam(GqlParams.StringParam.option, "option")
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N08)
                        .withParam(GqlParams.StringParam.option1, "myOption")
                        .withParam(GqlParams.StringParam.option2, "yourOption")
                        .build())
                .build();

        var cause = testException(causeGqlObject, "inner message");

        ErrorGqlStatusObject gqlObject = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N11)
                .withCause(cause)
                .build();

        ErrorGqlStatusObject almostIdenticalExceptionCauseGqlObject = ErrorGqlStatusObjectImplementation.from(
                        GqlStatusInfoCodes.STATUS_22N09)
                .withParam(GqlParams.StringParam.option, "option")
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N08)
                        .withParam(GqlParams.StringParam.option1, "myOption")
                        .withParam(GqlParams.StringParam.option2, "your-Option")
                        .build())
                .build();

        Throwable almostIdenticalCause = testException(almostIdenticalExceptionCauseGqlObject, "inner message");
        ErrorGqlStatusObject errorWithGqlExceptionCause = testException(gqlObject, "message", almostIdenticalCause);

        assertThat(errorWithGqlExceptionCause.cause()).isPresent();
        ErrorGqlStatusObject cause1 = errorWithGqlExceptionCause.cause().get();
        assertThat(cause1.gqlStatus()).isEqualTo(GqlStatusInfoCodes.STATUS_22N09.getStatusString());
        assertThat(cause1.statusDescription()).contains("conflicting values for 'option'.");

        assertThat(cause1.cause()).isPresent();
        ErrorGqlStatusObject cause2 = cause1.cause().get();
        assertThat(cause2.gqlStatus()).isEqualTo(GqlStatusInfoCodes.STATUS_22N08.getStatusString());
        assertThat(cause2.statusDescription()).contains("cannot combine 'myOption' with 'yourOption'");

        assertThat(cause2.cause()).isPresent();
        ErrorGqlStatusObject cause3 = cause2.cause().get();
        assertThat(cause3.gqlStatus()).isEqualTo(GqlStatusInfoCodes.STATUS_22N09.getStatusString());
        assertThat(cause3.statusDescription()).contains("conflicting values for 'option'.");

        assertThat(cause3.cause()).isPresent();
        ErrorGqlStatusObject cause4 = cause3.cause().get();
        assertThat(cause4.gqlStatus()).isEqualTo(GqlStatusInfoCodes.STATUS_22N08.getStatusString());
        assertThat(cause4.statusDescription()).contains("cannot combine 'myOption' with 'your-Option'");

        assertThat(cause4.cause()).isEmpty();
    }

    @Test
    void getMessageForTopLevelExceptionWithoutMessagePart() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22012)
                .build();
        var exception = testException(gql, "legacy message");

        assertThat(exception.getMessage()).isEqualTo("legacy message");
        assertThat(exception.gqlStatusObject().getMessage()).isEqualTo("22012");
    }

    @Test
    void getMessageShouldGiveNewMessageWhenFeatureFlagIsOn() {
        ErrorMessageHolder.USE_NEW_ERROR_MESSAGES = true;
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22012)
                .build();
        var exception = testException(gql, "legacy message");

        assertThat(exception.getMessage()).isEqualTo("22012");
        assertThat(exception.gqlStatusObject().getMessage()).isEqualTo("22012");
        ErrorMessageHolder.USE_NEW_ERROR_MESSAGES = false;
    }

    @Test
    void getMessageForTopLevelExceptionWithMessagePart() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N09)
                .withParam(GqlParams.StringParam.option, "myOption")
                .build();
        var exception = testException(gql, "legacy message");

        assertThat(exception.getMessage()).isEqualTo("legacy message");
        assertThat(exception.gqlStatusObject().getMessage())
                .isEqualTo(
                        "22N09: Invalid pre-parser option, cannot specify multiple conflicting values for 'myOption'.");
    }

    @Test
    void getMessageForTopLevelExceptionWithoutGql() {
        var exception = testException(null, "legacy message");

        assertThat(exception.getMessage()).isEqualTo("legacy message");
    }

    @Test
    void getMessageForErrorCauseWithoutMessagePart() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22012)
                        .build())
                .build();
        var exception = testException(gql, "legacy message");

        assertThat(exception.getMessage()).isEqualTo("legacy message");
        assertThat(exception.gqlStatusObject().getMessage()).isEqualTo("22000");
        assertThat(exception.cause()).isPresent();
        assertThat(exception.cause().get().getMessage()).isEqualTo("22012");
    }

    @Test
    void getMessageForErrorCauseWithMessagePart() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N09)
                        .withParam(GqlParams.StringParam.option, "myOption")
                        .build())
                .build();
        var exception = testException(gql, "legacy message");

        assertThat(exception.getMessage()).isEqualTo("legacy message");
        assertThat(exception.gqlStatusObject().getMessage()).isEqualTo("22000");
        assertThat(exception.cause()).isPresent();
        assertThat(exception.cause().get().getMessage())
                .isEqualTo(
                        "22N09: Invalid pre-parser option, cannot specify multiple conflicting values for 'myOption'.");
    }

    @Test
    void getMessageForJavaCause() {
        var gql1 = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .build();
        var gql2 = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N09)
                .withParam(GqlParams.StringParam.option, "myOption")
                .build();
        var innerException = testException(gql2, "legacy message 2");
        var exception = testException(gql1, "legacy message", innerException);

        assertThat(exception.getMessage()).isEqualTo("legacy message");
        assertThat(exception.gqlStatusObject().getMessage()).isEqualTo("22000");
        assertThat(exception.cause()).isPresent();
        assertThat(exception.cause().get().getMessage())
                .isEqualTo(
                        "22N09: Invalid pre-parser option, cannot specify multiple conflicting values for 'myOption'.");
    }

    @Test
    void getMessageForJavaCauseWithoutGql() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .build();

        // The top-level error has been implemented in the new framework, but the cause hasn't.
        var innerException = testException(null, "legacy message 2");
        var exception = testException(gql, "legacy message", innerException);

        assertThat(exception.getMessage()).isEqualTo("legacy message");
        assertThat(exception.gqlStatusObject().getMessage()).isEqualTo("22000");
        assertThat(exception.cause()).isPresent();
        assertThat(exception.cause().get().getMessage())
                .isEqualTo("50N42: Unexpected error has occurred. See debug log for details.");
    }

    @Test
    void shouldStoreClassificationsExceptUnknownInDiagnosticRecord() {
        var clientError = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .build();
        var clientException = testException(clientError, "legacy message");
        assertThat(clientException.diagnosticRecord())
                .containsEntry("_classification", ErrorClassification.CLIENT_ERROR.name());

        var dbError = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25000)
                .build();
        var dbException = testException(dbError, "legacy message");
        assertThat(dbException.diagnosticRecord())
                .containsEntry("_classification", ErrorClassification.DATABASE_ERROR.name());

        var transientError = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N00)
                .build();
        var transientException = testException(transientError, "legacy message");
        assertThat(transientException.diagnosticRecord())
                .containsEntry("_classification", ErrorClassification.TRANSIENT_ERROR.name());

        var unknownError = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08000)
                .build();
        var unknownException = testException(unknownError, "legacy message");
        assertThat(unknownException.diagnosticRecord()).doesNotContainKey("_classification");
    }

    @Test
    void shouldObfuscateStatusDescriptionIfGqlStatusRequiresObfuscation() {
        var errorGqlStatusObject = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N39)
                .withParam(GqlParams.StringParam.value, "{key: secret}")
                .build();
        var exception = testException(errorGqlStatusObject, "legacy message");
        assertThat(exception.obfuscatedStatusDescription())
                .isEqualTo(
                        "error: data exception - unsupported property value type. Value ****** cannot be stored in properties.");
    }

    @Test
    void shouldNotObfuscateStatusDescriptionWithoutParameters() {
        var errorGqlStatusObject = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22012)
                .build();
        var exception = testException(errorGqlStatusObject, "legacy message");
        assertThat(exception.obfuscatedStatusDescription()).isEqualTo("error: data exception - division by zero");
    }

    @Test
    void shouldNotObfuscateStatusDescriptionWhichIsMarkedAsNonSensitive() {
        var errorGqlStatusObject = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N01)
                .withParam(GqlParams.StringParam.db, "my_db")
                .withParam(GqlParams.StringParam.cfgSetting, "db.cfgSetting")
                .build();
        var exception = testException(errorGqlStatusObject, "legacy message");
        assertThat(exception.obfuscatedStatusDescription())
                .isEqualTo(
                        "error: connection exception - unable to write to database. Unable to write to database `my_db` on this server. Server-side routing is disabled. Either connect to the database leader directly or enable server-side routing by setting 'db.cfgSetting=true'.");
    }

    @Test
    void shouldObfuscateSensitiveParameters() {
        var errorGqlStatusObject = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N27)
                .withParam(GqlParams.StringParam.input, "{key: secret}")
                .withParam(GqlParams.StringParam.context, "some context")
                .withParam(GqlParams.ListParam.valueTypeList, List.of("STRING", "INTEGER", "FLOAT"))
                .withParam(GqlParams.StringParam.hint, " hint")
                .build();

        var exception = testException(errorGqlStatusObject, "legacy message");
        assertThat(exception.obfuscatedStatusDescription())
                .isEqualTo(
                        "error: data exception - invalid entity type. Invalid input '******' for some context. Expected to be STRING, INTEGER or FLOAT. hint");
    }
}
