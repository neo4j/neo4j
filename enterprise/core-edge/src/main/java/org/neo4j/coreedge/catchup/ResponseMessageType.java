/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.catchup;

import org.neo4j.coreedge.messaging.Message;

public enum ResponseMessageType implements Message
{
    TX( CURRENT_VERSION, (byte) 1 ),
    STORE_ID( CURRENT_VERSION, (byte) 2 ),
    FILE( CURRENT_VERSION, (byte) 3 ),
    STORE_COPY_FINISHED( CURRENT_VERSION, (byte) 4 ),
    CORE_SNAPSHOT( CURRENT_VERSION, (byte) 5 ),
    TX_STREAM_FINISHED( CURRENT_VERSION, (byte) 6 ),
    UNKNOWN( CURRENT_VERSION, (byte) 200 ),;

    private byte version;
    private byte messageType;

    ResponseMessageType( byte version, byte messageType )
    {
        this.version = version;
        this.messageType = messageType;
    }

    public static ResponseMessageType from( byte version, byte messageType )
    {
        if ( version != CURRENT_VERSION )
        {
            return UNKNOWN;
        }

        for ( ResponseMessageType responseMessageType : values() )
        {
            if ( responseMessageType.messageType == messageType )
            {
                return responseMessageType;
            }
        }
        return UNKNOWN;
    }

    @Override
    public byte version()
    {
        return version;
    }

    public byte messageType()
    {
        return messageType;
    }

    @Override
    public String toString()
    {
        return "ResponseMessageType{" + "version=" + version + ", messageType=" + messageType + '}';
    }
}
