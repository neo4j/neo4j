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
package org.neo4j.bolt.v5.request.messaging;

import org.neo4j.blob.Blob;
import org.neo4j.bolt.blob.BoltServerBlobIO;
import org.neo4j.bolt.v1.packstream.PackInput;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;
import org.neo4j.values.AnyValue;

import java.io.IOException;

public class Neo4jPackV5 extends Neo4jPackV2
{
    public static final long VERSION = 5;

    @Override
    public Packer newPacker( PackOutput output )
    {
        return new PackerV5( output );
    }

    @Override
    public Unpacker newUnpacker( PackInput input )
    {
        return new UnpackerV5( input );
    }

    @Override
    public long version()
    {
        return VERSION;
    }

    private static class PackerV5 extends Neo4jPackV2.PackerV2
    {
        PackerV5( PackOutput output )
        {
            super( output );
        }

        @Override
        public void writeBlob( Blob blob ) throws IOException
        {
            BoltServerBlobIO.packBlob( blob, out );
        }
    }

    private static class UnpackerV5 extends Neo4jPackV2.UnpackerV2
    {
        UnpackerV5( PackInput input )
        {
            super( input );
        }

        @Override
        public AnyValue unpack() throws IOException
        {
            AnyValue blobValue = BoltServerBlobIO.unpackBlob( this );
            if ( blobValue != null )
            {
                return blobValue;
            }
            return super.unpack();
        }
    }
}
