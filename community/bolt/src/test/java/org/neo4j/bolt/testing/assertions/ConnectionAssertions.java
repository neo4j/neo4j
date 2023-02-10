/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.testing.assertions;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;

public final class ConnectionAssertions extends AbstractAssert<ConnectionAssertions, Connection> {

    private ConnectionAssertions(Connection actual) {
        super(actual, ConnectionAssertions.class);
    }

    public static ConnectionAssertions assertThat(Connection actual) {
        return new ConnectionAssertions(actual);
    }

    public static InstanceOfAssertFactory<Connection, ConnectionAssertions> connection() {
        return new InstanceOfAssertFactory<>(Connection.class, ConnectionAssertions::new);
    }

    public ConnectionAssertions isIdling() {
        isNotNull();

        if (!this.actual.isIdling()) {
            failWithMessage("Expected connection to be idling");
        }

        return this;
    }

    public ConnectionAssertions isNotIdling() {
        isNotNull();

        if (this.actual.isIdling()) {
            failWithMessage("Expected connection to be busy");
        }

        return this;
    }

    public ConnectionAssertions hasPendingJobs() {
        isNotNull();

        if (!this.actual.hasPendingJobs()) {
            failWithMessage("Expected connection to have pending jobs");
        }

        return this;
    }

    public ConnectionAssertions hasNoPendingJobs() {
        isNotNull();

        if (this.actual.hasPendingJobs()) {
            failWithMessage("Expected connection to not have pending jobs");
        }

        return this;
    }

    public ConnectionAssertions inWorkerThread() {
        isNotNull();

        if (!this.actual.inWorkerThread()) {
            failWithMessage("Expected calling thread to be current worker thread");
        }

        return this;
    }

    public ConnectionAssertions notInWorkerThread() {
        isNotNull();

        if (this.actual.inWorkerThread()) {
            failWithMessage("Expected calling thread to not be current worker thread");
        }

        return this;
    }

    public ConnectionAssertions isInterrupted() {
        isNotNull();

        if (!this.actual.isInterrupted()) {
            failWithMessage("Expected connection to be interrupted");
        }

        return this;
    }

    public ConnectionAssertions isNotInterrupted() {
        isNotNull();

        if (this.actual.isInterrupted()) {
            failWithMessage("Expected connection to not be interrupted");
        }

        return this;
    }

    public ConnectionAssertions hasTransaction() {
        isNotNull();

        if (!this.actual.transaction().isPresent()) {
            failWithMessage("Expected connection to have a transaction");
        }

        return this;
    }

    public ConnectionAssertions hasNoTransaction() {
        isNotNull();

        if (this.actual.transaction().isPresent()) {
            failWithMessage("Expected connection to have no transaction");
        }

        return this;
    }

    public ConnectionAssertions isActive() {
        isNotNull();

        if (!this.actual.isActive()) {
            failWithMessage("Expected connection to be active");
        }

        return this;
    }

    public ConnectionAssertions isNotActive() {
        isNotNull();

        if (this.actual.isActive()) {
            failWithMessage("Expected connection to not be active");
        }

        return this;
    }

    public ConnectionAssertions isClosing() {
        isNotNull();

        if (!this.actual.isClosing()) {
            failWithMessage("Expected connection to be closing");
        }

        return this;
    }

    public ConnectionAssertions isNotClosing() {
        isNotNull();

        if (this.actual.isClosing()) {
            failWithMessage("Expected connection to not be closing");
        }

        return this;
    }

    public ConnectionAssertions isClosed() {
        isNotNull();

        if (!this.actual.isClosed()) {
            failWithMessage("Expected connection to be closed");
        }

        return this;
    }

    public ConnectionAssertions isNotClosed() {
        isNotNull();

        if (this.actual.isClosed()) {
            failWithMessage("Expected connection to not be closed");
        }

        return this;
    }
}
