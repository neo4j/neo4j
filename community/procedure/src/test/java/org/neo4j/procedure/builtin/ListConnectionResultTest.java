/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.procedure.builtin;

import io.netty.channel.Channel;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.transport.pipeline.ChannelProtector;
import org.neo4j.configuration.connectors.BoltConnector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ListConnectionResultTest
{
    @Test
    void buildResultOnConnectionWithoutClientAddress()
    {
        Channel channel = mock( Channel.class, RETURNS_MOCKS );
        when( channel.remoteAddress() ).thenReturn( null );
        BoltChannel boltChannel = new BoltChannel( "id", BoltConnector.NAME, channel, ChannelProtector.NULL );
        var result = new ListConnectionResult( boltChannel, ZoneId.systemDefault() );
        assertEquals( StringUtils.EMPTY, result.clientAddress );
    }
}
