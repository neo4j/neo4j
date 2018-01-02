/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class ClientCrashingWriter implements MadeUpWriter
{
    private final MadeUpClient client;
    private final int crashAtSize;
    private int totalSize;

    public ClientCrashingWriter( MadeUpClient client, int crashAtSize )
    {
        this.client = client;
        this.crashAtSize = crashAtSize;
    }

    @Override
    public void write( ReadableByteChannel data )
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect( 1000 );
        while ( true )
        {
            buffer.clear();
            try
            {
                int size = data.read( buffer );
                if ( size == -1 ) break;
                if ( (totalSize += size) >= crashAtSize ) client.stop();
            }
            catch ( IOException e )
            {
                throw new ComException( e );
            }
        }
    }
    
    public int getSizeRead()
    {
        return totalSize;
    }
}
