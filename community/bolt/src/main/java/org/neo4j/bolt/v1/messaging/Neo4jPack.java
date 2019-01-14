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
package org.neo4j.bolt.v1.messaging;

import java.io.IOException;

import org.neo4j.bolt.v1.packstream.PackInput;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

/**
 * Represents a single Bolt message format by exposing a {@link Packer packer} and {@link Unpacker unpacker}
 * for primitives of this format.
 */
public interface Neo4jPack
{
    interface Packer
    {
        void pack( String value ) throws IOException;

        void pack( AnyValue value ) throws IOException;

        void packStructHeader( int size, byte signature ) throws IOException;

        void packMapHeader( int size ) throws IOException;

        void packListHeader( int size ) throws IOException;

        void flush() throws IOException;
    }

    interface Unpacker
    {
        AnyValue unpack() throws IOException;

        String unpackString() throws IOException;

        MapValue unpackMap() throws IOException;

        long unpackStructHeader() throws IOException;

        char unpackStructSignature() throws IOException;

        long unpackListHeader() throws IOException;
    }

    Packer newPacker( PackOutput output );

    Unpacker newUnpacker( PackInput input );

    long version();
}
