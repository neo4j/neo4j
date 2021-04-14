/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.io.marshal;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

import static java.nio.charset.StandardCharsets.UTF_8;

public class StringChannelMarshal implements ChannelMarshal<String>
{
    public static final int NULL_STRING_LENGTH = -1;

    @Override
    public void marshal( String string, WritableChannel channel ) throws IOException
    {
        if ( string == null )
        {
            channel.putInt( NULL_STRING_LENGTH );
        }
        else
        {
            byte[] bytes = string.getBytes( UTF_8 );
            channel.putInt( bytes.length );
            channel.put( bytes, bytes.length );
        }
    }

    @Override
    public String unmarshal( ReadableChannel channel ) throws IOException
    {
        int len = channel.getInt();
        if ( len == NULL_STRING_LENGTH )
        {
            return null;
        }

        byte[] stringBytes = new byte[len];
        channel.get( stringBytes, stringBytes.length );

        return new String( stringBytes, UTF_8 );
    }
}
