/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.v1.runtime;

import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.values.AnyValue;

/**
 * Callback for handling the result of requests. For a given session, callbacks will be invoked serially,
 * in the order they were given. This means you may pass the same callback multiple times without waiting for a
 * reply, and are guaranteed that your callbacks will be called in order.
 */
public interface BoltResponseHandler
{
    /** Called exactly once, before the request is processed by the Session State Machine */
    void onStart();

    void onRecords( BoltResult result, boolean pull ) throws Exception;

    void onMetadata( String key, AnyValue value );

    /** Called when the state machine ignores an operation, because it is waiting for an error to be acknowledged */
    void markIgnored();

    /** Called zero or more times if there are failures */
    void markFailed( Neo4jError error );

    /** Called when the operation is completed. */
    void onFinish();

}
