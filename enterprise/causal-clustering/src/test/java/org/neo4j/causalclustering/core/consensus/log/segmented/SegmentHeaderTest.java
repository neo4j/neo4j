/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
