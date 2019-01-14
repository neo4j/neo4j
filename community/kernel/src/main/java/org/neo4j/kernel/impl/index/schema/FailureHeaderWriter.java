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
package org.neo4j.kernel.impl.index.schema;

import java.util.Arrays;
import java.util.function.Consumer;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.io.pagecache.PageCursor;

/**
 * Writes a failure message to a header in a {@link GBPTree}.
 */
class FailureHeaderWriter implements Consumer<PageCursor>
{
    /**
     * The {@code short} length field containing the length (number of bytes) of the failure message.
     */
    private static final int HEADER_LENGTH_FIELD_LENGTH = 2;

    private final byte[] failureBytes;

    FailureHeaderWriter( byte[] failureBytes )
    {
        this.failureBytes = failureBytes;
    }

    @Override
    public void accept( PageCursor cursor )
    {
        byte[] bytesToWrite = failureBytes;
        cursor.putByte( NativeSchemaIndexPopulator.BYTE_FAILED );
        int availableSpace = cursor.getCurrentPageSize() - cursor.getOffset();
        if ( bytesToWrite.length + HEADER_LENGTH_FIELD_LENGTH > availableSpace )
        {
            bytesToWrite = Arrays.copyOf( bytesToWrite, availableSpace - HEADER_LENGTH_FIELD_LENGTH );
        }
        cursor.putShort( (short) bytesToWrite.length );
        cursor.putBytes( bytesToWrite );
    }
}
