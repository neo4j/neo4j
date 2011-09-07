/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import java.nio.channels.FileChannel;

public class NullLogBuffer implements LogBuffer
{
    public static final LogBuffer INSTANCE = new NullLogBuffer();
    
    private NullLogBuffer() {}
    
    @Override public LogBuffer put( byte b ) throws IOException { return this; }
    @Override public LogBuffer putShort( short b ) throws IOException { return this; }
    @Override public LogBuffer putInt( int i ) throws IOException { return this; }
    @Override public LogBuffer putLong( long l ) throws IOException { return this; }
    @Override public LogBuffer putFloat( float f ) throws IOException { return this; }
    @Override public LogBuffer putDouble( double d ) throws IOException { return this; }
    @Override public LogBuffer put( byte[] bytes ) throws IOException { return this; }
    @Override public LogBuffer put( char[] chars ) throws IOException { return this; }
    @Override public void writeOut() throws IOException {}
    @Override public void force() throws IOException {}

    @Override
    public long getFileChannelPosition() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileChannel getFileChannel()
    {
        throw new UnsupportedOperationException();
    }
}
