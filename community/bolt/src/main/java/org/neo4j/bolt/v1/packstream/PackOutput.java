/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.bolt.v1.packstream;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This is where {@link PackStream} writes its output to.
 */
public interface PackOutput
{
    /** If implementation has been buffering data, it should flush those buffers now. */
    PackOutput flush() throws IOException;

    /** Produce a single byte */
    PackOutput writeByte( byte value ) throws IOException;

    /** Produce binary data */
    PackOutput writeBytes( ByteBuffer data ) throws IOException;

    /** Produce binary data */
    PackOutput writeBytes( byte[] data, int offset, int amountToWrite ) throws IOException;

    /** Produce a 4-byte signed integer */
    PackOutput writeShort( short value ) throws IOException;

    /** Produce a 4-byte signed integer */
    PackOutput writeInt( int value ) throws IOException;

    /** Produce an 8-byte signed integer */
    PackOutput writeLong( long value ) throws IOException;

    /** Produce an 8-byte IEEE 754 "double format" floating-point number */
    PackOutput writeDouble( double value ) throws IOException;
}
