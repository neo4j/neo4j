/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

public interface WritableLogChannel extends PositionAwareChannel, Closeable
{
    /**
    * Writes any changes not present in the channel yet and clears the buffer.
    */
    Flushable emptyBufferIntoChannelAndClearIt() throws IOException;

    WritableLogChannel put( byte value ) throws IOException;

    WritableLogChannel putShort( short value ) throws IOException;

    WritableLogChannel putInt( int value ) throws IOException;

    WritableLogChannel putLong( long value ) throws IOException;

    WritableLogChannel putFloat( float value ) throws IOException;

    WritableLogChannel putDouble( double value ) throws IOException;

    WritableLogChannel put( byte[] value, int length ) throws IOException;
}
