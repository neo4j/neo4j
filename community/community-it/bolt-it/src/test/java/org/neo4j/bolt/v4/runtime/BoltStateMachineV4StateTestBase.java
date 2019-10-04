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
package org.neo4j.bolt.v4.runtime;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.ResourceLock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.SessionExtension;
import org.neo4j.bolt.testing.BoltTestUtil;
import org.neo4j.bolt.v4.BoltProtocolV4;
import org.neo4j.bolt.v4.BoltStateMachineV4;
import org.neo4j.bolt.v4.messaging.BoltV4Messages;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;

@ResourceLock( "boltStateMachineV4" )
public class BoltStateMachineV4StateTestBase
{
    protected static final MapValue EMPTY_PARAMS = VirtualValues.EMPTY_MAP;
    protected static final BoltChannel BOLT_CHANNEL = BoltTestUtil.newTestBoltChannel( "conn-v4-test-boltchannel-id" );

    @RegisterExtension
    static final SessionExtension env = new SessionExtension();

    protected BoltStateMachineV4 newStateMachine()
    {
        return (BoltStateMachineV4) env.newMachine( BoltProtocolV4.VERSION, BOLT_CHANNEL );
    }

    protected BoltStateMachineV4 newStateMachineAfterAuth() throws BoltConnectionFatality
    {
        var machine = (BoltStateMachineV4) env.newMachine( BoltProtocolV4.VERSION, BOLT_CHANNEL );
        machine.process( BoltV4Messages.hello(), nullResponseHandler() );
        return machine;
    }

    protected static RequestMessage newHelloMessage()
    {
        return BoltV4Messages.hello();
    }

    protected static RequestMessage newPullMessage( long size ) throws BoltIOException
    {
        return BoltV4Messages.pull( size );
    }

    protected static RequestMessage newDiscardMessage( long size ) throws BoltIOException
    {
        return BoltV4Messages.discard( size );
    }
}
