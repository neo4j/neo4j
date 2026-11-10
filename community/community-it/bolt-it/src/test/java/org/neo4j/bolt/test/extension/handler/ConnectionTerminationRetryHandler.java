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
import org.neo4j.bolt.testing.client.error.BoltTestClientClosedException;

/**
 * Retries any tests which have failed to complete due to an abnormal connection termination up to
 * five times.
 * <p/>
 * This extension is automatically registered for all standard Bolt protocol and transport tests to
 * work around potential instability within CI environments under load where connections may be
 * improperly culled.
 */
public class ConnectionTerminationRetryHandler extends AbstractRetryingTestExecutionExceptionHandler {

    @Override
    protected String getMessage(ExtensionContext context, RetryInfo info, Throwable throwable) {
        return "Connection has been terminated - Database has likely gone away";
    }

    @Override
    protected boolean isRetryable(ExtensionContext context, RetryInfo info, Throwable cause) {
        return hasDirectCause(BoltTestClientClosedException.class, cause);
    }
}
