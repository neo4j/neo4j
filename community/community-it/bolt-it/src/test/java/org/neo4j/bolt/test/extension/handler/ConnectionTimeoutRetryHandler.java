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
package org.neo4j.bolt.test.extension.handler;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.bolt.test.extension.store.RetryInfo;
import org.neo4j.bolt.testing.client.error.BoltTestClientConnectionTimeoutException;
import org.neo4j.bolt.testing.client.error.BoltTestClientReadTimeoutException;
import org.neo4j.bolt.testing.client.error.BoltTestClientTimeoutException;
import org.neo4j.bolt.testing.client.error.BoltTestClientWriteTimeoutException;

/**
 * Retries any tests that have failed as a direct result of a timeout condition (e.g. the server has
 * failed to start, locked up or a network problem is preventing reliable communication).
 */
public class ConnectionTimeoutRetryHandler extends AbstractRetryingTestExecutionExceptionHandler {

    @Override
    protected String getMessage(ExtensionContext context, RetryInfo info, Throwable throwable) {
        if (throwable instanceof BoltTestClientConnectionTimeoutException) {
            return "Connection timeout - Database has gone away or failed to start up";
        }
        if (throwable instanceof BoltTestClientReadTimeoutException) {
            return "Read timeout - Database has gone away or locked up";
        }
        if (throwable instanceof BoltTestClientWriteTimeoutException) {
            return "Write timeout - Database has gone away or locked up";
        }

        return "Operation timeout - Database has gone away or locked up";
    }

    @Override
    protected boolean isRetryable(ExtensionContext context, RetryInfo info, Throwable cause) {
        return hasDirectCause(BoltTestClientTimeoutException.class, cause);
    }
}
