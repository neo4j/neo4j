/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import static org.neo4j.kernel.impl.storemigration.LegacyStore.*;

public class LegacyNodeStoreReader
{
    private String fileName;

    public LegacyNodeStoreReader( String fileName )
    {
        this.fileName = fileName;
    }

    public Iterable<NodeRecord> readNodeStore() throws IOException
    {
        FileChannel fileChannel = new RandomAccessFile( fileName, "r" ).getChannel();
        int recordLength = 9;
        int endHeaderSize = UTF8.encode( FROM_VERSION ).length;
        long recordCount = (fileChannel.size() - endHeaderSize) / recordLength;

        ByteBuffer buffer = ByteBuffer.allocateDirect( recordLength );

        ArrayList<NodeRecord> records = new ArrayList<NodeRecord>();
        for ( long id = 0; id < recordCount; id++ )
        {
            buffer.position( 0 );
            fileChannel.read( buffer );
            buffer.flip();
            long inUseByte = buffer.get();

            boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();
            if ( inUse )
            {
                long nextRel = getUnsignedInt( buffer );
                long nextProp = getUnsignedInt( buffer );

                long relModifier = (inUseByte & 0xEL) << 31;
                long propModifier = (inUseByte & 0xF0L) << 28;

                NodeRecord nodeRecord = new NodeRecord( id );
                nodeRecord.setInUse( inUse );
                nodeRecord.setNextRel( longFromIntAndMod( nextRel, relModifier ) );
                nodeRecord.setNextProp( longFromIntAndMod( nextProp, propModifier ) );

                records.add( nodeRecord );
            }
        }
        return records;
    }

}
