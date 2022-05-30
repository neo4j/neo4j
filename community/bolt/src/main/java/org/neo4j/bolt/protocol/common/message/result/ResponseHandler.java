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
package org.neo4j.bolt.protocol.common.message.result;

import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.values.AnyValue;

/**
 * Callback for handling the result of requests. For a given session, callbacks will be invoked serially, in the order they were given. This means you may pass
 * the same callback multiple times without waiting for a reply, and are guaranteed that your callbacks will be called in order.
 */
public interface ResponseHandler {
    /**
     * Callback to process a request to pull records from a result.
     * @param result the result to request records from.
     * @param size the number of records from the result to return.
     * @return true if there are more records remaining within the result.
     */
    boolean onPullRecords(BoltResult result, long size) throws Throwable;

    /**
     * Callback to process a request to discard records from a result.
     * @param result the result to discard records from.
     * @param size the number of records from the result to discard.
     * @return true if there are more records remaining within the result.
     */
    boolean onDiscardRecords(BoltResult result, long size) throws Throwable;

    void onMetadata(String key, AnyValue value);

    /**
     * Called when the state machine ignores an operation, because it is waiting for an error to be acknowledged
     */
    void markIgnored();

    /**
     * Called zero or more times if there are failures
     */
    void markFailed(Error error);

    /**
     * Called when the operation is completed.
     */
    void onFinish();
}
