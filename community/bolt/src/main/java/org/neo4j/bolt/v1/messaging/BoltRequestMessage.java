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

import static java.lang.String.format;

/**
 * Enumeration representing all defined Bolt request messages.
 * Also contains the signature byte with which the message is
 * encoded on the wire.
 */
public enum BoltRequestMessage
{
    INIT( 0x01 ),
    ACK_FAILURE( 0x0E ),
    RESET( 0x0F ),
    RUN( 0x10 ),
    DISCARD_ALL( 0x2F ),
    PULL_ALL( 0x3F );

    private static BoltRequestMessage[] valuesBySignature = new BoltRequestMessage[0x40];
    static
    {
        for ( BoltRequestMessage value : values() )
        {
            valuesBySignature[value.signature()] = value;
        }
    }

    /**
     * Obtain a request message by signature.
     *
     * @param signature the signature byte to look up
     * @return the appropriate message instance
     * @throws IllegalArgumentException if no such message exists
     */
    public static BoltRequestMessage withSignature( int signature )
    {
        BoltRequestMessage message = valuesBySignature[signature];
        if ( message == null )
        {
            throw new IllegalArgumentException( format( "No message with signature %d", signature ) );
        }
        return message;
    }

    private final byte signature;

    BoltRequestMessage( int signature )
    {
        this.signature = (byte) signature;
    }

    public byte signature()
    {
        return signature;
    }

}
