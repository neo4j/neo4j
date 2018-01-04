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
package org.neo4j.bolt.v1.messaging;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.neo4j.bolt.v1.packstream.PackInput;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

public interface Neo4jPack
{
    interface Packer
    {
        void packStructHeader( int size, byte signature ) throws IOException;

        void pack( AnyValue value ) throws IOException;

        void packRawMap( MapValue map ) throws IOException;

        void packMapHeader( int size ) throws IOException;

        void flush() throws IOException;

        void pack( String value ) throws IOException;

        void packListHeader( int size ) throws IOException;

        void consumeError() throws BoltIOException;
    }

    interface Unpacker
    {
        boolean hasNext() throws IOException;

        long unpackStructHeader() throws IOException;

        char unpackStructSignature() throws IOException;

        String unpackString() throws IOException;

        Map<String,Object> unpackToRawMap() throws IOException;

        MapValue unpackMap() throws IOException;

        long unpackListHeader() throws IOException;

        AnyValue unpack() throws IOException;

        Optional<Neo4jError> consumeError();
    }

    Packer newPacker( PackOutput output );

    Unpacker newUnpacker( PackInput input );
}
