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
package org.neo4j.kernel.impl.index.schema;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_FAILED;

class NativeIndexHeaderReaderTest
{
    @Test
    void mustReportFailedIfNoHeader()
    {
        ByteBuffer emptyBuffer = ByteBuffer.wrap( new byte[0] );
        NativeIndexHeaderReader nativeIndexHeaderReader = new NativeIndexHeaderReader( NO_HEADER_READER );
        nativeIndexHeaderReader.read( emptyBuffer );
        assertSame( BYTE_FAILED, nativeIndexHeaderReader.state );
        assertThat( nativeIndexHeaderReader.failureMessage,
                containsString( "Could not read header, most likely caused by index not being fully constructed. Index needs to be recreated. Stacktrace:" ) );
    }

    @Test
    void mustReportFailedIfHeaderTooShort()
    {
        ByteBuffer emptyBuffer = ByteBuffer.wrap( new byte[1] );
        NativeIndexHeaderReader nativeIndexHeaderReader = new NativeIndexHeaderReader( ByteBuffer::get );
        nativeIndexHeaderReader.read( emptyBuffer );
        assertSame( BYTE_FAILED, nativeIndexHeaderReader.state );
        assertThat( nativeIndexHeaderReader.failureMessage,
                containsString( "Could not read header, most likely caused by index not being fully constructed. Index needs to be recreated. Stacktrace:" ) );
    }

    @Test
    void mustNotThrowIfHeaderLongEnough()
    {
        ByteBuffer emptyBuffer = ByteBuffer.wrap( new byte[1] );
        NativeIndexHeaderReader nativeIndexHeaderReader = new NativeIndexHeaderReader( NO_HEADER_READER );
        nativeIndexHeaderReader.read( emptyBuffer );
    }
}
