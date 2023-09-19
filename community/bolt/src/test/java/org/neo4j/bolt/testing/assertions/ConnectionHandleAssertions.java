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

import org.assertj.core.api.InstanceOfAssertFactory;
import org.neo4j.bolt.protocol.common.connector.connection.ConnectionHandle;

public final class ConnectionHandleAssertions
        extends AbstractConnectionAssertions<ConnectionHandleAssertions, ConnectionHandle> {

    private ConnectionHandleAssertions(ConnectionHandle actual) {
        super(actual, ConnectionHandleAssertions.class);
    }

    public static ConnectionHandleAssertions assertThat(ConnectionHandle actual) {
        return new ConnectionHandleAssertions(actual);
    }

    public static InstanceOfAssertFactory<ConnectionHandle, ConnectionHandleAssertions> connection() {
        return new InstanceOfAssertFactory<>(ConnectionHandle.class, ConnectionHandleAssertions::new);
    }

    public ConnectionHandleAssertions hasTransaction() {
        var tx = this.actual.transaction();

        if (!tx.isPresent()) {
            this.failWithMessage("Expected transaction to be present");
        }

        return this;
    }

    public ConnectionHandleAssertions hasNoTransaction() {
        var tx = this.actual.transaction();

        if (tx.isPresent()) {
            this.failWithMessage("Expected no transaction to be present");
        }

        return this;
    }
}
