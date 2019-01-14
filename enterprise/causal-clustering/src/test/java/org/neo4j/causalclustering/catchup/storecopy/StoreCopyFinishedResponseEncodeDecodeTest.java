/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class StoreCopyFinishedResponseEncodeDecodeTest
{
    @Test
    public void shouldEncodeAndDecodePullRequestMessage()
    {
        // given
        EmbeddedChannel channel =
                new EmbeddedChannel( new StoreCopyFinishedResponseEncoder(), new StoreCopyFinishedResponseDecoder() );
        StoreCopyFinishedResponse sent = new StoreCopyFinishedResponse( Status.E_STORE_ID_MISMATCH );

        // when
        channel.writeOutbound( sent );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        StoreCopyFinishedResponse received = channel.readInbound();
        assertNotSame( sent, received );
        assertEquals( sent, received );
    }

}
