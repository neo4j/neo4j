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

import java.util.Objects;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;

public final class ClientConnectionInfoAssertions
        extends AbstractAssert<ClientConnectionInfoAssertions, ClientConnectionInfo> {

    private ClientConnectionInfoAssertions(ClientConnectionInfo actual) {
        super(actual, ClientConnectionInfoAssertions.class);
    }

    public static ClientConnectionInfoAssertions assertThat(ClientConnectionInfo actual) {
        return new ClientConnectionInfoAssertions(actual);
    }

    public static InstanceOfAssertFactory<ClientConnectionInfo, ClientConnectionInfoAssertions> clientConnectionInfo() {
        return new InstanceOfAssertFactory<>(ClientConnectionInfo.class, ClientConnectionInfoAssertions::new);
    }

    public ClientConnectionInfoAssertions hasProtocol(String expected) {
        isNotNull();

        if (!Objects.equals(this.actual.protocol(), expected)) {
            failWithActualExpectedAndMessage(
                    this.actual.protocol(),
                    expected,
                    "Expected protocol <%s> but got <%s>",
                    expected,
                    this.actual.protocol());
        }

        return this;
    }

    public ClientConnectionInfoAssertions hasConnectionId(String expected) {
        isNotNull();

        if (!Objects.equals(this.actual.connectionId(), expected)) {
            failWithActualExpectedAndMessage(
                    this.actual.connectionId(),
                    expected,
                    "Expected connection id <%s> but got <%s>",
                    expected,
                    this.actual.connectionId());
        }

        return this;
    }

    public ClientConnectionInfoAssertions hasClientAddress(String expected) {
        isNotNull();

        if (!Objects.equals(this.actual.clientAddress(), expected)) {
            failWithActualExpectedAndMessage(
                    this.actual.clientAddress(),
                    expected,
                    "Expected client address <%s> but got <%s>",
                    expected,
                    this.actual.clientAddress());
        }

        return this;
    }

    public ClientConnectionInfoAssertions hasRequestURI(String expected) {
        isNotNull();

        if (!Objects.equals(this.actual.requestURI(), expected)) {
            failWithActualExpectedAndMessage(
                    this.actual.requestURI(),
                    expected,
                    "Expected request URI <%s> but got <%s>",
                    expected,
                    this.actual.requestURI());
        }

        return this;
    }
}
