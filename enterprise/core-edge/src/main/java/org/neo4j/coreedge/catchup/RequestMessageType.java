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

public enum RequestMessageType implements MessageType
{
    TX_PULL_REQUEST( CURRENT_VERSION, (byte) 1 ),
    STORE( CURRENT_VERSION, (byte) 2 ),
    RAFT_STATE( CURRENT_VERSION, (byte) 3 ),
    STORE_ID( CURRENT_VERSION, (byte) 4 ),
    UNKNOWN( CURRENT_VERSION, (byte) 404 );

    private byte version;
    private byte type;

    RequestMessageType( byte version, byte type )
    {
        this.version = version;
        this.type = type;
    }

    public static RequestMessageType from( byte version, byte type )
    {
        if ( version != CURRENT_VERSION )
        {
            return UNKNOWN;
        }

        for ( RequestMessageType responseMessageType : values() )
        {
            if ( responseMessageType.type == type )
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

    @Override
    public byte type()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return "RequestMessageType{" + "version=" + version + ", type=" + type + '}';
    }

    public static class Encoder extends MessageTypeEncoder<RequestMessageType>{}
}
