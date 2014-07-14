/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.io.pagecache.impl.standard;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * <strong>Implementation note:</strong> These methods must NEVER swallow a thread-interrupt.
 * If the thread is interrupted when these methods are called, or gets interrupted while they are
 * executing, then they must either throw an InterruptedException, or leave the interrupted-status
 * flag alone.
 */
public interface PageSwapper
{
    void read( long pageId, ByteBuffer into ) throws IOException;
    void write( long pageId, ByteBuffer from ) throws IOException;

    /**
     * Notification that a page has been evicted, used to clean up state in structures
     * outside the page table.
     */
    void evicted( long pageId );

    String fileName();
}
