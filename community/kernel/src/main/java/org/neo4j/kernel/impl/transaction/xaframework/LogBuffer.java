/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;

import org.neo4j.kernel.impl.nioneo.store.StoreChannel;

public interface LogBuffer
{
    LogBuffer put( byte b ) throws IOException;

    LogBuffer putShort( short b ) throws IOException;

    LogBuffer putInt( int i ) throws IOException;

    LogBuffer putLong( long l ) throws IOException;

    LogBuffer putFloat( float f ) throws IOException;

    LogBuffer putDouble( double d ) throws IOException;

    LogBuffer put( byte[] bytes ) throws IOException;

    LogBuffer put( char[] chars ) throws IOException;

    /**
     * Makes sure the data added to this buffer is written out to the underlying
     * file. Makes sure that readers of the channel will see the content of the
     * buffer up until the time of this call.
     *
     * @throws IOException if the data couldn't be written.
     */
    void writeOut() throws IOException;

    /**
     * Makes sure the data added to this buffer is written out to the underlying
     * file and forced. Same guarantees as writeOut() plus actually being
     * written to disk.
     * 
     * @throws IOException if the data couldn't be written.
     */
    void force() throws IOException;

    long getFileChannelPosition() throws IOException;

    StoreChannel getFileChannel();
}
