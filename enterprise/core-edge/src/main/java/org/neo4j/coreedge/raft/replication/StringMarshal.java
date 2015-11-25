/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.replication;

import java.io.UnsupportedEncodingException;

import io.netty.buffer.ByteBuf;

public class StringMarshal
{
    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final int NULL_STRING_LENGTH = -1;

    public static void serialize( ByteBuf buffer, String string )
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

    public static String deserialize( ByteBuf buffer )
    {
        try
        {
            int len = buffer.readInt();
            if ( len == NULL_STRING_LENGTH )
            {
                return null;
            }
            ByteBuf stringBytes = buffer.readBytes( len );

            return new String( stringBytes.array(), DEFAULT_CHARSET );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( "UTF-8 should be supported by all java platforms." );
        }
    }
}
