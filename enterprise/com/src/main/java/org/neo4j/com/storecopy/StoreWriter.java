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
package org.neo4j.com.storecopy;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public interface StoreWriter extends Closeable
{
    /**
     * Pipe the data from the given {@link ReadableByteChannel} to a location given by the {@code path}, using the
     * given {@code temporaryBuffer} for buffering if necessary.
     * The {@code hasData} is an effect of the block format not supporting a zero length blocks, whereas a neostore
     * file may actually be 0 bytes we'll have to keep track of that special case.
     * The {@code requiredElementAlignment} parameter specifies the size in bytes to which the transferred elements
     * should be aligned. For record store files, this is the record size. For files that have no special alignment
     * requirements, you should use the value {@code 1} to signify that any alignment will do.
     */
    long write( String path, ReadableByteChannel data, ByteBuffer temporaryBuffer, boolean hasData,
                int requiredElementAlignment ) throws IOException;

    @Override
    void close();
}
