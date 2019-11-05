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
package org.neo4j.io.fs;

import java.io.IOException;

/**
 * Extends a {@link WritableChannel} and {@link FlushableChannel} with a way of calculating checksum.
 */
public interface FlushableChecksumChannel extends FlushableChannel, WritableChecksumChannel
{
    @Override
    FlushableChecksumChannel put( byte value ) throws IOException;

    @Override
    FlushableChecksumChannel putShort( short value ) throws IOException;

    @Override
    FlushableChecksumChannel putInt( int value ) throws IOException;

    @Override
    FlushableChecksumChannel putLong( long value ) throws IOException;

    @Override
    FlushableChecksumChannel putFloat( float value ) throws IOException;

    @Override
    FlushableChecksumChannel putDouble( double value ) throws IOException;

    @Override
    FlushableChecksumChannel put( byte[] value, int length ) throws IOException;
}
