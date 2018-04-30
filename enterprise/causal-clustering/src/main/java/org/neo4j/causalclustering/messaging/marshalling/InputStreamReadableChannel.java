/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.messaging.marshalling;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.neo4j.storageengine.api.ReadableChannel;

public class InputStreamReadableChannel implements ReadableChannel
{
    private final DataInputStream dataInputStream;

    public InputStreamReadableChannel( InputStream inputStream )
    {
        this.dataInputStream = new DataInputStream( inputStream );
    }

    @Override
    public byte get() throws IOException
    {
        return dataInputStream.readByte();
    }

    @Override
    public short getShort() throws IOException
    {
        return dataInputStream.readShort();
    }

    @Override
    public int getInt() throws IOException
    {
        return dataInputStream.readInt();
    }

    @Override
    public long getLong() throws IOException
    {
        return dataInputStream.readLong();
    }

    @Override
    public float getFloat() throws IOException
    {
        return dataInputStream.readFloat();
    }

    @Override
    public double getDouble() throws IOException
    {
        return dataInputStream.readDouble();
    }

    @Override
    public void get( byte[] bytes, int length ) throws IOException
    {
        dataInputStream.read( bytes, 0, length );
    }
}
