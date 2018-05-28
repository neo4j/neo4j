/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering;

import java.util.ArrayList;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

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

        VersionDecoder versionDecoder = new VersionDecoder(logProvider, currentVersion );

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

        VersionDecoder versionDecoder = new VersionDecoder(logProvider, currentVersion );

        ByteBuf incoming = Unpooled.buffer();
        incoming.writeByte( currentVersion );

        // when
        versionDecoder.decode( null, incoming, new ArrayList<>() );

        // then
        logProvider.assertNoLoggingOccurred();
    }
}
