/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class StringMarshal
{
    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final int NULL_STRING_LENGTH = -1;

    private StringMarshal()
    {
    }

    public static void marshal( ByteBuf buffer, String string )
    {
        try
        {
            if ( string == null )
            {
                buffer.writeInt( NULL_STRING_LENGTH );
            }
            else
            {
                byte[] bytes = string.getBytes( DEFAULT_CHARSET );
                buffer.writeInt( bytes.length );
                buffer.writeBytes( bytes );
            }

        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( "UTF-8 should be supported by all java platforms." );
        }
    }

    public static void marshal( ByteBuffer buffer, String string )
    {
        try
        {
            if ( string == null )
            {
                buffer.putInt( NULL_STRING_LENGTH );
            }
            else
            {
                byte[] bytes = string.getBytes( DEFAULT_CHARSET );
                buffer.putInt( bytes.length );
                buffer.put( bytes );
            }

        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( "UTF-8 should be supported by all java platforms." );
        }
    }

    public static String unmarshal( ByteBuf buffer )
    {
        try
        {
            int len = buffer.readInt();
            if ( len == NULL_STRING_LENGTH )
            {
                return null;
            }

            byte[] bytes = new byte[len];
            buffer.readBytes( bytes );
            return new String( bytes, DEFAULT_CHARSET );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( "UTF-8 should be supported by all java platforms." );
        }
    }

    public static void marshal( WritableChannel channel, String string ) throws IOException
    {
        try
        {
            if ( string == null )
            {
                channel.putInt( NULL_STRING_LENGTH );
            }
            else
            {
                byte[] bytes = string.getBytes( DEFAULT_CHARSET );
                channel.putInt( bytes.length );
                channel.put( bytes, bytes.length );
            }

        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( "UTF-8 should be supported by all java platforms." );
        }
    }

    public static String unmarshal( ReadableChannel channel ) throws IOException
    {
        try
        {
            int len = channel.getInt();
            byte[] stringBytes = new byte[len];
            channel.get( stringBytes, stringBytes.length );

            return new String( stringBytes, DEFAULT_CHARSET );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( "UTF-8 should be supported by all java platforms." );
        }
    }
}
