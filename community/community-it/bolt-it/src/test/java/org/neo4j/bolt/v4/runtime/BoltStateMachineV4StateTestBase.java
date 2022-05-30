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
package org.neo4j.bolt.v4.runtime;

import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.protocol.v40.BoltProtocolV40;
import org.neo4j.bolt.protocol.v40.fsm.StateMachineV40;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.SessionExtension;
import org.neo4j.bolt.testing.BoltChannelFactory;
import org.neo4j.bolt.testing.messages.BoltV40Messages;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

@ResourceLock("boltStateMachineV4")
public class BoltStateMachineV4StateTestBase {
    protected static final MapValue EMPTY_PARAMS = VirtualValues.EMPTY_MAP;
    protected static final BoltChannel BOLT_CHANNEL =
            BoltChannelFactory.newTestBoltChannel("conn-v4-test-boltchannel-id");
    protected static final MemoryTracker MEMORY_TRACKER = EmptyMemoryTracker.INSTANCE;

    @RegisterExtension
    static final SessionExtension env = new SessionExtension();

    protected StateMachineV40 newStateMachine() {
        return (StateMachineV40) env.newMachine(BoltProtocolV40.VERSION, BOLT_CHANNEL);
    }

    protected StateMachineV40 newStateMachineAfterAuth() throws BoltConnectionFatality {
        return newStateMachineAfterAuth(env);
    }

    protected StateMachineV40 newStateMachineAfterAuth(String connectionId) throws BoltConnectionFatality {
        var machine = (StateMachineV40)
                env.newMachine(BoltProtocolV40.VERSION, BoltChannelFactory.newTestBoltChannel(connectionId));
        machine.process(BoltV40Messages.hello(), nullResponseHandler());
        return machine;
    }

    // TODO: Remove me >:(
    protected static StateMachineV40 newStateMachineAfterAuth(SessionExtension env) throws BoltConnectionFatality {
        var machine = (StateMachineV40) env.newMachine(BoltProtocolV40.VERSION, BOLT_CHANNEL);
        machine.process(BoltV40Messages.hello(), nullResponseHandler());
        return machine;
    }
}
