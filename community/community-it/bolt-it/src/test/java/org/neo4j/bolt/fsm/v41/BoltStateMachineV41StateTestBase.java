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
package org.neo4j.bolt.fsm.v41;

import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v41.BoltProtocolV41;
import org.neo4j.bolt.protocol.v41.fsm.StateMachineV41;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.SessionExtension;
import org.neo4j.bolt.testing.messages.BoltV40Messages;
import org.neo4j.bolt.testing.messages.BoltV41Messages;

@ResourceLock("boltStateMachineV41")
public class BoltStateMachineV41StateTestBase {

    @RegisterExtension
    static final SessionExtension env = new SessionExtension();

    protected StateMachineV41 newStateMachine() {
        return (StateMachineV41) env.newMachine(BoltProtocolV41.VERSION);
    }

    protected StateMachineV41 newStateMachineAfterAuth() throws BoltConnectionFatality {
        var machine = (StateMachineV41) env.newMachine(BoltProtocolV41.VERSION);
        machine.process(BoltV40Messages.hello(), nullResponseHandler());
        return machine;
    }

    protected static RequestMessage newHelloMessage() {
        return BoltV41Messages.hello();
    }

    protected static RequestMessage newPullMessage(long size) throws BoltIOException {
        return BoltV41Messages.pull(size);
    }

    protected static RequestMessage newDiscardMessage(long size) throws BoltIOException {
        return BoltV41Messages.discard(size);
    }
}
