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
package org.neo4j.bolt.testing.assertions;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.neo4j.bolt.protocol.common.message.response.IgnoredMessage;
import org.neo4j.bolt.protocol.common.message.response.ResponseMessage;

public abstract class ResponseMessageAssertions<
                SELF extends AbstractAssert<SELF, ACTUAL>, ACTUAL extends ResponseMessage>
        extends AbstractAssert<SELF, ACTUAL> {

    protected ResponseMessageAssertions(ACTUAL actual, Class<?> selfType) {
        super(actual, selfType);
    }

    public static GenericResponseMessageAssertions assertThat(ResponseMessage actual) {
        return new GenericResponseMessageAssertions(actual);
    }

    public static InstanceOfAssertFactory<ResponseMessage, GenericResponseMessageAssertions> responseMessage() {
        return new InstanceOfAssertFactory<>(ResponseMessage.class, ResponseMessageAssertions::assertThat);
    }

    @SuppressWarnings("unchecked")
    public SELF hasSignature(byte expected) {
        this.isNotNull();

        if (this.actual.signature() != expected) {
            this.failWithActualExpectedAndMessage(
                    this.actual.signature(),
                    expected,
                    "Expected response message with signature <%d> but got <%d>",
                    expected,
                    this.actual.signature());
        }

        return (SELF) this;
    }

    public SuccessMessageAssertions isSuccessResponse() {
        return this.asInstanceOf(SuccessMessageAssertions.successMessage());
    }

    public FailureMessageAssertions isFailureResponse() {
        return this.asInstanceOf(FailureMessageAssertions.failureMessage());
    }

    @SuppressWarnings("unchecked")
    public SELF isIgnoredResponse() {
        this.isNotNull();

        this.isInstanceOf(IgnoredMessage.class);

        return (SELF) this;
    }

    public static final class GenericResponseMessageAssertions
            extends ResponseMessageAssertions<GenericResponseMessageAssertions, ResponseMessage> {

        private GenericResponseMessageAssertions(ResponseMessage actual) {
            super(actual, GenericResponseMessageAssertions.class);
        }
    }
}
