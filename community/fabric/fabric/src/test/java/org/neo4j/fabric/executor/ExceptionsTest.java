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
package org.neo4j.fabric.executor;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.neo4j.kernel.api.exceptions.Status.General.InvalidArguments;
import static org.neo4j.kernel.api.exceptions.Status.Statement.ConstraintVerificationFailed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.ConstraintViolationException;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.Status;

class ExceptionsTest {

    @Test
    void gqlFallbackUnexpectedError() {
        var transformedException =
                Exceptions.transformUnexpectedError(Status.General.UnknownError, new RuntimeException("msg-1"));
        assertThat(unpackExceptionMessages(transformedException)).contains("msg-1");
        assertThat(transformedException).isInstanceOf(ErrorGqlStatusObject.class);
        var transformedExceptionGql = (ErrorGqlStatusObject) transformedException;
        assertThat(transformedExceptionGql.gqlStatus()).isEqualTo("50N42");
    }

    @Test
    void gqlFallbackTransactionStartFailure() {
        var transformedException = Exceptions.transformTransactionStartFailure(new RuntimeException("msg-1"));
        assertThat(unpackExceptionMessages(transformedException)).contains("msg-1");
        assertThat(transformedException).isInstanceOf(ErrorGqlStatusObject.class);
        var transformedExceptionGql = (ErrorGqlStatusObject) transformedException;
        assertThat(transformedExceptionGql.gqlStatus()).isEqualTo("25N06");
    }

    @Test
    void compositeGqlExceptionTranslation() {
        var gqlException = InvalidArgumentsException.internalAlterServer("server");
        var translatedGqlException = FabricException.translateLocalError(gqlException);
        assertThat(translatedGqlException.gqlStatus()).isEqualTo("50N00");
        assertThat(translatedGqlException.status()).isEqualTo(InvalidArguments);
        assertThat(translatedGqlException.getMessage())
                .isEqualTo("Server 'server' can't be altered: must specify options");
    }

    @Test
    void compositeExceptionTranslationForExceptionWithoutGqlStatus() {
        var notGqlException = new ConstraintViolationException("message", null);
        var translatedGqlException = FabricException.translateLocalError(notGqlException);
        assertThat(translatedGqlException.gqlStatus()).isEqualTo("50N42");
        assertThat(translatedGqlException.status()).isEqualTo(ConstraintVerificationFailed);
        assertThat(translatedGqlException.getMessage()).isEqualTo("message");
    }

    private static List<String> unpackExceptionMessages(Exception exception) {
        List<String> messages = new ArrayList<>();
        messages.add(exception.getMessage());
        Arrays.stream(exception.getSuppressed()).map(Throwable::getMessage).forEach(messages::add);
        return messages;
    }
}
