/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;

public class LegacyPropertyIndexStoreReader
{
    public static final String FROM_VERSION = "PropertyIndex v0.9.9";
    private File fileName;

    public LegacyPropertyIndexStoreReader( File fileName )
    {
        this.fileName = fileName;
    }

    public Iterable<PropertyIndexRecord> readPropertyIndexStore() throws IOException
    {
        FileChannel fileChannel = new RandomAccessFile( fileName, "r" ).getChannel();
        int recordLength = 9;
        int endHeaderSize = UTF8.encode( FROM_VERSION ).length;
        long recordCount = (fileChannel.size() - endHeaderSize) / recordLength;

        LinkedList<PropertyIndexRecord> records = new LinkedList<PropertyIndexRecord>();

        ByteBuffer buffer = ByteBuffer.allocateDirect( recordLength );

        for ( int id = 0; id <= recordCount; id++ )
        {
            buffer.position( 0 );
            fileChannel.read( buffer );
            buffer.flip();
            long inUseByte = buffer.get();

            boolean inUse = inUseByte == Record.IN_USE.byteValue();
            if (inUse) {
                PropertyIndexRecord record = new PropertyIndexRecord( id );
                record.setInUse( inUse );
                record.setPropertyCount( buffer.getInt() );
                record.setNameId( buffer.getInt() );
                records.add( record );
            }
        }
        fileChannel.close();
        return records;
    }
}
