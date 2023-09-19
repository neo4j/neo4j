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
import org.neo4j.bolt.protocol.common.connector.connection.Connection;

public abstract class AbstractConnectionAssertions<
                SELF extends AbstractConnectionAssertions<SELF, ACTUAL>, ACTUAL extends Connection>
        extends AbstractAssert<SELF, ACTUAL> {

    protected AbstractConnectionAssertions(ACTUAL actual, Class<?> selfType) {
        super(actual, selfType);
    }

    @SuppressWarnings("unchecked")
    public SELF isIdling() {
        isNotNull();

        if (!this.actual.isIdling()) {
            failWithMessage("Expected connection to be idling");
        }

        return (SELF) this;
    }

    @SuppressWarnings("unchecked")
    public SELF isNotIdling() {
        isNotNull();

        if (this.actual.isIdling()) {
            failWithMessage("Expected connection to be busy");
        }

        return (SELF) this;
    }

    @SuppressWarnings("unchecked")
    public SELF hasPendingJobs() {
        isNotNull();

        if (!this.actual.hasPendingJobs()) {
            failWithMessage("Expected connection to have pending jobs");
        }

        return (SELF) this;
    }

    @SuppressWarnings("unchecked")
    public SELF hasNoPendingJobs() {
        isNotNull();

        if (this.actual.hasPendingJobs()) {
            failWithMessage("Expected connection to not have pending jobs");
        }

        return (SELF) this;
    }

    @SuppressWarnings("unchecked")
    public SELF inWorkerThread() {
        isNotNull();

        if (!this.actual.inWorkerThread()) {
            failWithMessage("Expected calling thread to be current worker thread");
        }

        return (SELF) this;
    }

    @SuppressWarnings("unchecked")
    public SELF notInWorkerThread() {
        isNotNull();

        if (this.actual.inWorkerThread()) {
            failWithMessage("Expected calling thread to not be current worker thread");
        }

        return (SELF) this;
    }

    @SuppressWarnings("unchecked")
    public SELF isInterrupted() {
        isNotNull();

        if (!this.actual.isInterrupted()) {
            failWithMessage("Expected connection to be interrupted");
        }

        return (SELF) this;
    }

    @SuppressWarnings("unchecked")
    public SELF isNotInterrupted() {
        isNotNull();

        if (this.actual.isInterrupted()) {
            failWithMessage("Expected connection to not be interrupted");
        }

        return (SELF) this;
    }

    @SuppressWarnings("unchecked")
    public SELF isActive() {
        isNotNull();

        if (!this.actual.isActive()) {
            failWithMessage("Expected connection to be active");
        }

        return (SELF) this;
    }

    @SuppressWarnings("unchecked")
    public SELF isNotActive() {
        isNotNull();

        if (this.actual.isActive()) {
            failWithMessage("Expected connection to not be active");
        }

        return (SELF) this;
    }

    @SuppressWarnings("unchecked")
    public SELF isClosing() {
        isNotNull();

        if (!this.actual.isClosing()) {
            failWithMessage("Expected connection to be closing");
        }

        return (SELF) this;
    }

    @SuppressWarnings("unchecked")
    public SELF isNotClosing() {
        isNotNull();

        if (this.actual.isClosing()) {
            failWithMessage("Expected connection to not be closing");
        }

        return (SELF) this;
    }

    @SuppressWarnings("unchecked")
    public SELF isClosed() {
        isNotNull();

        if (!this.actual.isClosed()) {
            failWithMessage("Expected connection to be closed");
        }

        return (SELF) this;
    }

    @SuppressWarnings("unchecked")
    public SELF isNotClosed() {
        isNotNull();

        if (this.actual.isClosed()) {
            failWithMessage("Expected connection to not be closed");
        }

        return (SELF) this;
    }
}
