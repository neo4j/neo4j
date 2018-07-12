/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.bolt.v3.runtime.integration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.v3.BoltProtocolV3;
import org.neo4j.bolt.v3.BoltStateMachineV3;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BoltStateMachineStateTestBase
{
    protected static final MapValue EMPTY_PARAMS = VirtualValues.EMPTY_MAP;
    protected static final String USER_AGENT = "BoltConnectionIT/0.0";
    protected static final BoltChannel boltChannel = mock( BoltChannel.class );

    @BeforeEach
    void setup()
    {
        when( boltChannel.id() ).thenReturn( "conn-v3-test-boltchannel-id" );
    }

    @RegisterExtension
    static final SessionExtension env = new SessionExtension();

    protected BoltStateMachineV3 newStateMachine()
    {
        return (BoltStateMachineV3) env.newMachine( BoltProtocolV3.VERSION, boltChannel );
    }

    protected static HelloMessage newHelloMessage()
    {
        return new HelloMessage( MapUtil.map( "user_agent", USER_AGENT ) );
    }
}
