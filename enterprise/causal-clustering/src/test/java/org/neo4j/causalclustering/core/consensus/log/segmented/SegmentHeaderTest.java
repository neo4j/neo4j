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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;

public class SegmentHeaderTest
{
    private SegmentHeader.Marshal marshal = new SegmentHeader.Marshal();

    @Test
    public void shouldWriteAndReadHeader() throws Exception
    {
        // given
        long prevFileLastIndex = 1;
        long version = 2;
        long prevIndex = 3;
        long prevTerm = 4;

        SegmentHeader writtenHeader = new SegmentHeader( prevFileLastIndex, version, prevIndex, prevTerm );

        InMemoryClosableChannel channel = new InMemoryClosableChannel();

        // when
        marshal.marshal( writtenHeader, channel );
        SegmentHeader readHeader = marshal.unmarshal( channel );

        // then
        assertEquals( writtenHeader, readHeader );
    }

    @Test
    public void shouldThrowExceptionWhenReadingIncompleteHeader() throws Exception
    {
        // given
        long prevFileLastIndex = 1;
        long version = 2;
        long prevIndex = 3;
        long prevTerm = 4;

        SegmentHeader writtenHeader = new SegmentHeader( prevFileLastIndex, version, prevIndex, prevTerm );
        InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.putLong( writtenHeader.version() );
        channel.putLong( writtenHeader.prevIndex() );

        // when
        try
        {
            marshal.unmarshal( channel );
            fail();
        }
        catch ( EndOfStreamException e )
        {
            // expected
        }
    }
}
