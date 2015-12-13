/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.command;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;

import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.read2bLengthAndString;

public class PhysicalLogCommandReaderV2_2_4 extends PhysicalLogCommandReaderV2_2
{
    @Override
    protected Map<String,Integer> readMap( ReadableLogChannel channel ) throws IOException
    {
        byte size = channel.get();
        Map<String,Integer> result = new HashMap<>();
        for ( int i = 0; i < size; i++ )
        {
            String key = read2bLengthAndString( channel );
            int id = getUnsignedShort( channel );
            if ( key == null )
            {
                return null;
            }
            result.put( key, id );
        }
        return result;
    }

    private int getUnsignedShort( ReadableLogChannel channel ) throws IOException
    {
        int result = channel.getShort() & 0xFFFF;
        return result == 0xFFFF ? -1 : result;
    }

    /**
     * CHANGE: byte->unsigned short for indexNameId and keyId
     */
    @Override
    protected IndexCommandHeader readIndexCommandHeader( ReadableLogChannel channel ) throws IOException
    {
        byte firstHeaderByte = channel.get();
        byte valueType = (byte) ((firstHeaderByte & 0x1C) >> 2);
        byte entityType = (byte) ((firstHeaderByte & 0x2) >> 1);
        boolean entityIdNeedsLong = (firstHeaderByte & 0x1) > 0;

        byte secondHeaderByte = channel.get();
        boolean startNodeNeedsLong = (secondHeaderByte & 0x80) > 0;
        boolean endNodeNeedsLong = (secondHeaderByte & 0x40) > 0;

        int indexNameId = getUnsignedShort( channel );
        int keyId = getUnsignedShort( channel );
        return new IndexCommandHeader( valueType, entityType, entityIdNeedsLong,
                indexNameId, startNodeNeedsLong, endNodeNeedsLong, keyId );
    }
}
