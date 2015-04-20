/*
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
package org.neo4j.packstream;

import java.io.IOException;

/**
 * Client is responsible for calling {@link #ensure(int)} before any calls to 'put' operations,
 * other than to {@link #put(byte[], int, int)}.
 */
public interface PackOutput
{
    /** Ensure at least the given set of bytes can be written. Size will never exceed 16 */
    PackOutput ensure( int size ) throws IOException;

    PackOutput flush() throws IOException;

    PackOutput put( byte value );

    PackOutput put( byte[] data, int offset, int amountToWrite ) throws IOException;

    PackOutput putShort( short value );

    PackOutput putInt( int value );

    PackOutput putLong( long value );

    PackOutput putDouble( double value );
}
