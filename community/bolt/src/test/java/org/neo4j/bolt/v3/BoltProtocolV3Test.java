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
package org.neo4j.bolt.v3;

import org.junit.jupiter.api.Test;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltStateMachineFactory;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;
import org.neo4j.bolt.v3.messaging.BoltRequestMessageReaderV3;
import org.neo4j.logging.internal.NullLogService;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

class BoltProtocolV3Test
{
    @Test
    void shouldCreatePackForBoltV3() throws Throwable
    {
        BoltProtocolV3 protocolV3 =
                new BoltProtocolV3( mock( BoltChannel.class ), ( ch, st ) -> mock( BoltConnection.class ), mock( BoltStateMachineFactory.class ),
                        NullLogService.getInstance() );

        assertThat( protocolV3.createPack(), instanceOf( Neo4jPackV2.class ) );
    }

    @Test
    void shouldVersionReturnBoltV3() throws Throwable
    {
        BoltProtocolV3 protocolV3 =
                new BoltProtocolV3( mock( BoltChannel.class ), ( ch, st ) -> mock( BoltConnection.class ), mock( BoltStateMachineFactory.class ),
                        NullLogService.getInstance() );

        assertThat( protocolV3.version(), equalTo( 3L ) );
    }

    @Test
    void shouldCreateMessageReaderForBoltV3() throws Throwable
    {
        BoltProtocolV3 protocolV3 =
                new BoltProtocolV3( mock( BoltChannel.class ), ( ch, st ) -> mock( BoltConnection.class ), mock( BoltStateMachineFactory.class ),
                        NullLogService.getInstance() );

        assertThat( protocolV3.createMessageReader( mock( BoltChannel.class ), mock( Neo4jPack.class ), mock( BoltConnection.class ),
                NullLogService.getInstance() ), instanceOf( BoltRequestMessageReaderV3.class ) );
    }
}
