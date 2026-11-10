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
package org.neo4j.bolt.test.extension.error;

import org.neo4j.bolt.test.extension.store.RetryInfo;
import org.opentest4j.TestAbortedException;

/**
 * Signals to the Bolt test support extension that a test has been aborted and should be retried due
 * to a temporary network condition or otherwise permissible condition.
 */
public class TestRetryException extends TestAbortedException {

    private final RetryInfo retryInfo;

    public TestRetryException(RetryInfo retryInfo, String message, Throwable cause) {
        super("Retrying test (" + retryInfo + ") due to permissible failure: " + message, cause);
        this.retryInfo = retryInfo;
    }

    public RetryInfo getRetryInfo() {
        return this.retryInfo;
    }
}
