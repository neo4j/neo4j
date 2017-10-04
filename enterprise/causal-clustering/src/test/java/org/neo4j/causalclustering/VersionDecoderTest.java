/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.util.ArrayList;

import org.neo4j.logging.AssertableLogProvider;

import static org.neo4j.logging.AssertableLogProvider.inLog;

public class VersionDecoderTest
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Test
    public void shouldDiscardMessageWithUnknownVersionAndLogAnError() throws Exception
    {
        // given
        byte currentVersion = (byte) 1;
        byte messageVersion = (byte) 0;

        VersionDecoder versionDecoder = new VersionDecoder( logProvider, currentVersion );

        ByteBuf incoming = Unpooled.buffer();
        incoming.writeByte( messageVersion );

        // when
        versionDecoder.decode( null, incoming, new ArrayList<>() );

        // then
        logProvider.assertExactly( inLog( versionDecoder.getClass() )
                .error( "Unsupported version %d, current version is %d", messageVersion, currentVersion  ));
    }

    @Test
    public void shouldHandleMessageWithCorrectVersion() throws Exception
    {
        // given
        byte currentVersion = (byte) 1;

        VersionDecoder versionDecoder = new VersionDecoder( logProvider, currentVersion );

        ByteBuf incoming = Unpooled.buffer();
        incoming.writeByte( currentVersion );

        // when
        versionDecoder.decode( null, incoming, new ArrayList<>() );

        // then
        logProvider.assertNoLoggingOccurred();
    }
}
