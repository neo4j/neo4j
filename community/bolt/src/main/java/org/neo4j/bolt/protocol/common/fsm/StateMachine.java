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
package org.neo4j.bolt.protocol.common.fsm;

import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.result.ResponseHandler;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.exceptions.KernelException;
import org.neo4j.util.VisibleForTesting;

public interface StateMachine extends AutoCloseable {
    @VisibleForTesting
    Connection connection();

    void process(RequestMessage message, ResponseHandler handler) throws BoltConnectionFatality;

    boolean shouldStickOnThread();

    void validateTransaction() throws KernelException;

    boolean hasOpenStatement();

    void interrupt();

    boolean reset() throws BoltConnectionFatality;

    void markFailed(Error error);

    void handleFailure(Throwable cause, boolean fatal) throws BoltConnectionFatality;

    void handleExternalFailure(Error error, ResponseHandler handler) throws BoltConnectionFatality;

    void markForTermination();

    boolean isClosed();

    @Override
    void close();
}
