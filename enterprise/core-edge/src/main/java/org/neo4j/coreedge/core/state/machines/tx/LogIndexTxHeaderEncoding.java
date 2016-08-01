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
package org.neo4j.coreedge.core.state.machines.tx;

/**
 * Log index is encoded in the header of transactions in the transaction log.
 */
public class LogIndexTxHeaderEncoding
{
    public static byte[] encodeLogIndexAsTxHeader( long logIndex )
    {
        byte[] b = new byte[Long.BYTES];
        for ( int i = Long.BYTES - 1; i > 0; i-- )
        {
            b[i] = (byte) logIndex;
            logIndex >>>= Byte.SIZE;
        }
        b[0] = (byte) logIndex;
        return b;
    }

    public static long decodeLogIndexFromTxHeader( byte[] bytes )
    {
        if ( bytes.length < Long.BYTES )
        {
            throw new IllegalArgumentException( "Unable to decode RAFT log index from transaction header" );
        }

        long logIndex = 0;
        for ( int i = 0; i < Long.BYTES; i++ )
        {
            logIndex <<= Byte.SIZE;
            logIndex ^= bytes[i] & 0xFF;
        }
        return logIndex;
    }
}
