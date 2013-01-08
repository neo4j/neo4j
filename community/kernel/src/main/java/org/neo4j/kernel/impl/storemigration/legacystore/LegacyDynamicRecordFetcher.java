/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.legacystore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.util.StringLogger;

public class LegacyDynamicRecordFetcher
{
    private LegacyDynamicStoreReader stringPropertyStore;
    private LegacyDynamicStoreReader arrayPropertyStore;

    public LegacyDynamicRecordFetcher(String stringStoreFileName, String arrayStoreFileName) throws IOException
    {
        this(stringStoreFileName, arrayStoreFileName, StringLogger.DEV_NULL);
    }

    public LegacyDynamicRecordFetcher( String stringStoreFileName, String arrayStoreFileName, StringLogger log ) throws IOException
    {
        stringPropertyStore = new LegacyDynamicStoreReader( stringStoreFileName, LegacyDynamicStoreReader.FROM_VERSION_STRING, log );
        arrayPropertyStore = new LegacyDynamicStoreReader( arrayStoreFileName, LegacyDynamicStoreReader.FROM_VERSION_ARRAY, log );
    }

    public List<LegacyDynamicRecord> readDynamicRecords( LegacyPropertyRecord record )
    {
        if ( record.getType() == LegacyPropertyType.STRING )
        {
            List<LegacyDynamicRecord> stringRecords =
                    stringPropertyStore.getPropertyChain(
                            record.getPropBlock() );
            for ( LegacyDynamicRecord stringRecord : stringRecords )
            {
                stringRecord.setType( PropertyType.STRING.intValue() );
            }
            return stringRecords;
        } else if ( record.getType() == LegacyPropertyType.ARRAY )
        {
            List<LegacyDynamicRecord> arrayRecords =
                    arrayPropertyStore.getPropertyChain(
                            record.getPropBlock() );
            for ( LegacyDynamicRecord arrayRecord : arrayRecords )
            {
                arrayRecord.setType( PropertyType.ARRAY.intValue() );
            }
            return arrayRecords;
        }
        return null;
    }

    public Object getStringFor( LegacyPropertyRecord propRecord )
    {
        long startRecordId = propRecord.getPropBlock();
        List<LegacyDynamicRecord> legacyDynamicRecords = readDynamicRecords( propRecord );
        return joinRecordsIntoString( startRecordId, legacyDynamicRecords );
    }

    public static String joinRecordsIntoString( long startRecordId, List<LegacyDynamicRecord> legacyDynamicRecords )
    {
        Map<Long, LegacyDynamicRecord> recordsMap = new HashMap<Long, LegacyDynamicRecord>();
        long recordToFind = startRecordId;
        for ( LegacyDynamicRecord record : legacyDynamicRecords )
        {
            recordsMap.put( record.getId(), record );
        }
        List<char[]> charList = new LinkedList<char[]>();
        while ( recordToFind != Record.NO_NEXT_BLOCK.intValue() )
        {
            LegacyDynamicRecord record = recordsMap.get( recordToFind );
            if ( !record.isCharData() )
            {
                ByteBuffer buf = ByteBuffer.wrap( record.getData() );
                char[] chars = new char[record.getData().length / 2];
                buf.asCharBuffer().get( chars );
                charList.add( chars );
            } else
            {
                charList.add( record.getDataAsChar() );
            }
            recordToFind = record.getNextBlock();
        }
        StringBuffer buf = new StringBuffer();
        for ( char[] str : charList )
        {
            buf.append( str );
        }
        return buf.toString();
    }

    public Object getArrayFor( LegacyPropertyRecord propertyRecord )
    {
        long recordToFind = propertyRecord.getPropBlock();
        Map<Long, LegacyDynamicRecord> recordsMap = new HashMap<Long, LegacyDynamicRecord>();
        for ( LegacyDynamicRecord record : readDynamicRecords( propertyRecord ) )
        {
            recordsMap.put( record.getId(), record );
        }
        List<byte[]> byteList = new LinkedList<byte[]>();
        int totalSize = 0;
        while ( recordToFind != Record.NO_NEXT_BLOCK.intValue() )
        {
            LegacyDynamicRecord record = recordsMap.get( recordToFind );
            if ( !record.isCharData() )
            {
                ByteBuffer buf = ByteBuffer.wrap( record.getData() );
                byte[] bytes = new byte[record.getData().length];
                totalSize += bytes.length;
                buf.get( bytes );
                byteList.add( bytes );
            } else
            {
                throw new InvalidRecordException(
                        "Expected byte data on record " + record );
            }
            recordToFind = record.getNextBlock();
        }
        byte[] bArray = new byte[totalSize];
        int offset = 0;
        for ( byte[] currentArray : byteList )
        {
            System.arraycopy( currentArray, 0, bArray, offset,
                    currentArray.length );
            offset += currentArray.length;
        }
        return arrayPropertyStore.getRightArray( bArray );
    }

    public void close() throws IOException
    {
        arrayPropertyStore.close();
        stringPropertyStore.close();
    }
}
