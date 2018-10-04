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
package org.neo4j.causalclustering.identity;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.UUID;

import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.causalclustering.messaging.marshalling.InputStreamReadableChannel;
import org.neo4j.causalclustering.messaging.marshalling.OutputStreamWritableChannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class ClusterIdMarshalTest
{
    private ChannelMarshal<ClusterId> marshal = ClusterId.Marshal.INSTANCE;

    @Test
    public void shouldMarshalClusterId() throws Throwable
    {
        // given
        ClusterId original = new ClusterId( UUID.randomUUID() );
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        // when
        OutputStreamWritableChannel writableChannel = new OutputStreamWritableChannel( outputStream );
        marshal.marshal( original, writableChannel );

        InputStreamReadableChannel readableChannel = new InputStreamReadableChannel( new ByteArrayInputStream( outputStream.toByteArray() ) );
        ClusterId result = marshal.unmarshal( readableChannel );

        // then
        assertNotSame( original, result );
        assertEquals( original, result );
    }

    @Test
    public void shouldMarshalNullClusterId() throws Throwable
    {
        // given
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        // when
        OutputStreamWritableChannel writableChannel = new OutputStreamWritableChannel( outputStream );
        marshal.marshal( null, writableChannel );

        InputStreamReadableChannel readableChannel = new InputStreamReadableChannel( new ByteArrayInputStream( outputStream.toByteArray() ) );
        ClusterId result = marshal.unmarshal( readableChannel );

        // then
        assertNull( result );
    }
}
