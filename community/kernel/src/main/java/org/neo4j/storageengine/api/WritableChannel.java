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
package org.neo4j.storageengine.api;

import java.io.IOException;

/**
 * Represents an infinite channel to write primitive data to.
 */
public interface WritableChannel
{
    /**
     * Writes a {@code byte} to this channel.
     *
     * @param value byte value.
     * @return this channel, for fluent usage.
     * @throws IOException if I/O error occurs.
     */
    WritableChannel put( byte value ) throws IOException;

    /**
     * Writes a {@code short} to this channel.
     *
     * @param value short value.
     * @return this channel, for fluent usage.
     * @throws IOException if I/O error occurs.
     */
    WritableChannel putShort( short value ) throws IOException;

    /**
     * Writes a {@code int} to this channel.
     *
     * @param value int value.
     * @return this channel, for fluent usage.
     * @throws IOException if I/O error occurs.
     */
    WritableChannel putInt( int value ) throws IOException;

    /**
     * Writes a {@code long} to this channel.
     *
     * @param value long value.
     * @return this channel, for fluent usage.
     * @throws IOException if I/O error occurs.
     */
    WritableChannel putLong( long value ) throws IOException;

    /**
     * Writes a {@code float} to this channel.
     *
     * @param value float value.
     * @return this channel, for fluent usage.
     * @throws IOException if I/O error occurs.
     */
    WritableChannel putFloat( float value ) throws IOException;

    /**
     * Writes a {@code double} to this channel.
     *
     * @param value double value.
     * @return this channel, for fluent usage.
     * @throws IOException if I/O error occurs.
     */
    WritableChannel putDouble( double value ) throws IOException;

    /**
     * Writes a {@code byte[]} to this channel.
     *
     * @param value byte array.
     * @param length number of items of the array to write.
     * @return this channel, for fluent usage.
     * @throws IOException if I/O error occurs.
     */
    WritableChannel put( byte[] value, int length ) throws IOException;
}
